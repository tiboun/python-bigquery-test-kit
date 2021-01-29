# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

import uuid
from copy import deepcopy
from functools import reduce
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from google.cloud.bigquery import Client
from google.cloud.bigquery.job import (QueryJob, QueryJobConfig,
                                       WriteDisposition)
from google.cloud.bigquery.query import UDFResource
from google.cloud.bigquery.schema import SchemaField
from logzero import logger

from bq_test_kit.bq_dsl.bq_query_datum import BQQueryDatum
from bq_test_kit.bq_dsl.bq_query_results import BQQueryResult
from bq_test_kit.bq_dsl.bq_resources import BaseBQResource, Project, Table
from bq_test_kit.bq_dsl.schema_mixin import SchemaMixin
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.constants import (DEFAULT_JOB_ID_PREFIX,
                                   DEFAULT_TECHNICAL_COLUMN_PREFIX)
from bq_test_kit.data_literal_transformers.base_data_literal_transformer import \
    BaseDataLiteralTransformer
from bq_test_kit.interpolators.base_interpolator import BaseInterpolator
from bq_test_kit.resource_loaders import BaseResourceLoader
from bq_test_kit.typing import (QueryParameter, SchemaFieldTypedDatum,
                                TableResources)


class BQQueryTemplate(SchemaMixin):
    """Query DSL which allows query to be interpolated before its execution.
    """

    def __init__(self,
                 *, from_: Union[BaseResourceLoader, str], bqtk_config: BQTestKitConfig,
                 location: Optional[str] = None, bq_client: Client,
                 job_config: QueryJobConfig = None, project: Project = None,
                 interpolators: List[BaseInterpolator] = None, global_dict: Dict[str, Any] = None,
                 temp_tables: List[Tuple[BaseDataLiteralTransformer, TableResources]] = None,
                 temp_technical_column_prefix: str = DEFAULT_TECHNICAL_COLUMN_PREFIX) -> None:
        """Constructor of BQQueryTemplate

        Args:
            from_ (Union[BaseResourceLoader, str]): query to load from.
            bqtk_config (BQTestKitConfig): config used accross the query DSL.
            bq_client (Client): instance of bigquery client to use accross the datasetL.
            location (Optional[str], optional): force location for dataset. Defaults extracted from bqtk_config.
            job_config (QueryJobConfig, optional): Configure job. Defaults to QueryJobConfig().
            project (Project, optional): project in which this query should be run.
                Allows usage of relative table name. Defaults to None.
            interpolators (List[BaseInterpolator], optional): List of interpolator to use before running query.
                Defaults to None.
            global_dict (Dict[str, Any], optional): global dictionary to mix with local interpolator's dictionary.
                Defaults to None.
            temp_tables (List[Tuple[BaseDataLiteralTransformer, TableResources]]):
                list of all table to create as temp table with a data literal.
                Defaults to None.
            temp_technical_column_prefix (str):
                prefix used when renaming partition column which are invalid in bigquery.
                Defaults to bq_test_kit.constants.DEFAULT_TECHNICAL_COLUMN_PREFIX.
        """
        self.from_ = from_
        self._bq_client = bq_client
        self.job_config = job_config if job_config else QueryJobConfig()
        self.location = location if location else bqtk_config.get_default_location()
        self.project = project
        self.interpolators = interpolators if interpolators else []
        self.bqtk_config = bqtk_config
        self.global_dict = global_dict if global_dict else {}
        self.temp_tables = ([self._to_temp_tables_with_schema_field(temp_table) for temp_table in temp_tables]
                            if temp_tables else [])
        self.temp_technical_column_prefix = temp_technical_column_prefix

    def run(self) -> BQQueryResult:
        """Execute the query and return a BQQueryResult.

        Returns:
            BQQueryResult: results are stored in this object.
        """
        temp_table_queries, create_statements, drop_statements, nb_statements = self._generate_temp_tables()
        interpolated_query = self._interpolate(temp_table_queries)
        effective_query = create_statements + interpolated_query + drop_statements
        logger.debug("Query rendered as :\n%s", effective_query)
        query_job: QueryJob = self._bq_client.query(
            effective_query,
            job_id_prefix=DEFAULT_JOB_ID_PREFIX,
            job_config=self.job_config,
            location=self.location,
            project=self.project.fqdn() if self.project else None
        )
        logger.info("Job id is : %s", query_job.job_id)
        row_iterator = query_job.result(max_results=0 if self.job_config.destination else None)
        if nb_statements > 0:
            query_jobs = self._bq_client.list_jobs(parent_job=query_job.job_id)
            job_ids = []
            jobs_with_step = []
            for qjob in query_jobs:
                job_ids.append(qjob.job_id)
                jobs_with_step.append((int(qjob.job_id[qjob.job_id.rindex("_") + 1:]), qjob))
            logger.info("Jobs id occured in this query are %s", ", ".join(job_ids))
            jobs_with_step = sorted(jobs_with_step, key=lambda j: j[0], )
            last_user_statement_job = jobs_with_step[-nb_statements-1][1]
            logger.debug("selected job for result is %s", last_user_statement_job.job_id)
            row_iterator = last_user_statement_job.result(max_results=0 if self.job_config.destination else None)
        return BQQueryResult(row_iterator)

    def allow_large_results(self, allow: bool) -> 'BQQueryTemplate':
        """Allow large query results tables (legacy SQL, only)

        Args:
            allow (bool): allow or not

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with allow_large_results set.
        """
        query_template = deepcopy(self)
        query_template.job_config.allow_large_results = allow
        return query_template

    def with_destination(self, table: Table, partition: str = None) -> 'BQQueryTemplate':
        """Set a destination where the result of the query should be stored.
           When destination is set, no rows is returned.

        Args:
            table (Table): table to write to.
            partition (str): partition decorator.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with destination set.
        """
        query_template = deepcopy(self)
        fqdn = table.fqdn()
        _partition = "$" + partition if partition else ""
        target = fqdn + _partition
        query_template.job_config.destination = target
        return query_template

    def with_query_parameters(self, params: List[QueryParameter]) -> 'BQQueryTemplate':
        """Set query parameters when query contains parameters.

        Args:
            params (List[QueryParameter]): list of parameters.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with query parameters set.
        """
        query_template = deepcopy(self)
        query_template.job_config.query_parameters = params
        return query_template

    def with_udf_resources(self, udf_resources: List[UDFResource]) -> 'BQQueryTemplate':
        """Set udf resources to use along with the query.

        Args:
            udf_resources (List[UDFResource]): list of udf resources to use.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with a list of udf resources set.
        """
        query_template = deepcopy(self)
        query_template.job_config.udf_resources = udf_resources
        return query_template

    def add_udf_resource(self, udf_resource: UDFResource) -> 'BQQueryTemplate':
        """Add udf resource to the existing list of udf_resources.

        Args:
            udf_resources (UDFResource): an udf resources to add into existing list.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with an updated list of udf resources.
        """
        query_template = deepcopy(self)
        udfs = query_template.job_config.udf_resources if query_template.job_config.udf_resources else []
        udfs = udfs + [udf_resource]
        query_template.job_config.udf_resources = udfs
        return query_template

    def use_legacy_sql(self, use: bool = True) -> 'BQQueryTemplate':
        """Use legacy sql syntax instead of standard sql.

        Args:
            use (bool, optional): use legacy sql. Defaults to True.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with legacy sql usage set.
        """
        query_template = deepcopy(self)
        query_template.job_config.use_legacy_sql = use
        return query_template

    def use_query_cache(self, use: bool = True) -> 'BQQueryTemplate':
        """Look for the query result in the cache.

        Args:
            use (bool, optional): use query result in the cache. Defaults to True.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with cache usage set.
        """
        query_template = deepcopy(self)
        query_template.job_config.use_query_cache = use
        return query_template

    def overwrite(self) -> 'BQQueryTemplate':
        """Truncate destination if it exists.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with data overwrite set.
        """
        query_template = deepcopy(self)
        query_template.job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        return query_template

    def append(self) -> 'BQQueryTemplate':
        """Append data to the destination.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with data append set.
        """
        query_template = deepcopy(self)
        query_template.job_config.write_disposition = WriteDisposition.WRITE_APPEND
        return query_template

    def error_if_exists(self) -> 'BQQueryTemplate':
        """Throw error if destination has data already.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with error to thrown if data exists.
        """
        query_template = deepcopy(self)
        query_template.job_config.write_disposition = WriteDisposition.WRITE_EMPTY
        return query_template

    def with_interpolators(self, renderers: List[BaseInterpolator]) -> 'BQQueryTemplate':
        """Interpolators used in order to interpolate query template before its execution.

        Args:
            renderers (List[BaseInterpolator]): list of interpolators to use.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with a list of interpolators.
        """
        query_template = deepcopy(self)
        query_template.interpolators = renderers
        return query_template

    def add_interpolator(self, renderer: BaseInterpolator) -> 'BQQueryTemplate':
        """Add interpolator to existing list of interpolators.

        Args:
            renderer (BaseInterpolator): an interpolator to use

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with an updated list of interpolators.
        """
        query_template = deepcopy(self)
        query_template.interpolators = query_template.interpolators if query_template.interpolators else []
        query_template.interpolators.append(renderer)
        return query_template

    def with_global_dict(self, global_dict: Dict[str, Any]) -> 'BQQueryTemplate':
        """Set global dictionary to use with interpolators.

        Args:
            global_dict (Dict[str, Any]): global dictionary to use with interpolators.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with global dict overwritten.
        """
        query_template = deepcopy(self)
        query_template.global_dict = global_dict
        return query_template

    def update_global_dict(self, dict_resource: Union[Dict[str, Any], List[BaseBQResource]]) -> 'BQQueryTemplate':
        """Update global dictionary with either a dictionary or a list of BaseBQResource.
           When a list of BaseBQResource is given see _default_resource_to_kv

        Args:
            dict_resource (Union[Dict[str, Any], List[BaseBQResource]]): [description]

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with global dict updated.
        """
        if isinstance(dict_resource, dict):
            return self.update_global_dict_with_dict(dict_resource)
        return self.update_global_dict_with_bq_resources(dict_resource)

    def update_global_dict_with_dict(self, dict_update: Dict[str, Any]) -> 'BQQueryTemplate':
        """Update global dictionary with a dictionary.

        Args:
            dict_update (Dict[str, Any]): dict to update with.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with global dict updated.
        """
        query_template = deepcopy(self)
        query_template.global_dict.update(dict_update)
        return query_template

    def update_global_dict_with_bq_resources(self, bq_resources: List[BaseBQResource],
                                             resource_to_kv: Callable[[BaseBQResource], Tuple[str, Any]] = None
                                             ) -> 'BQQueryTemplate':
        """Update global dictionary with a list of BaseBQResource.

        Args:
            bq_resources (List[BaseBQResource]): list of base BaseBQResource to build dict and update global dict.
            resource_to_kv (Callable[[BaseBQResource], Tuple[str, Any]], optional):
                Lambda that transform BaseBQResource to a dictionary.
                Defaults to _default_resource_to_kv.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with global dict updated.
        """
        query_template = deepcopy(self)
        to_kv = resource_to_kv if resource_to_kv else self._default_resource_to_kv
        local_dict = dict([to_kv(bq_resource) for bq_resource in bq_resources])
        query_template.global_dict.update(local_dict)
        return query_template

    def with_temp_tables(self, tables: Tuple[BaseDataLiteralTransformer, TableResources]) -> 'BQQueryTemplate':
        """add data as temporary tables. table names will be registered as dict entries.

        Args:
            tables (Tuple[BaseDataLiteralTransformer, TableResources]):
                literal transformer along with their datum and schema.
                Each of them will be a temp table.

        Returns:
            BQQueryTemplate: new instance with temp_tables filled
        """
        query_template = deepcopy(self)
        query_template.temp_tables.append(self._to_temp_tables_with_schema_field(tables))
        return query_template

    def with_datum(self, tables: TableResources) -> BQQueryDatum:
        """Go to the datum DSL which will enrich current query template.

        Args:
            tables (TableResources): tables to register in the query.

        Returns:
            BQQueryDatum: dsl to register datum.
        """
        return BQQueryDatum(self, True, tables)

    def _to_temp_tables_with_schema_field(self,
                                          tables: Tuple[BaseDataLiteralTransformer, TableResources]
                                          ) -> SchemaFieldTypedDatum:
        table_dict = {name: (datum, self.to_schema_field_list(schema)) for name, (datum, schema) in tables[1].items()}
        return (tables[0], table_dict)

    def with_temp_technical_column_prefix(self, prefix: str) -> 'BQQueryTemplate':
        """Change technical column prefix.

        Args:
            prefix (str): technical column prefix to use when renaming them.

        Returns:
            BQQueryTemplate: new instance of current Query template with column prefix updated.
        """
        query_template = deepcopy(self)
        query_template.temp_technical_column_prefix = prefix
        return query_template

    def _interpolate(self, temp_table_queries) -> str:
        query = self.from_ if isinstance(self.from_, str) else self.from_.load()
        merged_global_dict = deepcopy(self.global_dict)
        merged_global_dict.update(temp_table_queries)
        body = reduce(lambda template, interpolator: interpolator.interpolate(template, merged_global_dict),
                      self.interpolators, query)
        if not body.strip().endswith(";") and self.temp_tables:
            body = body + ";"
        return body

    def _rename_technical_column(self, name: str) -> str:
        if (name.upper().startswith("_PARTITION") or
           name.upper().startswith("_TABLE_") or
           name.upper().startswith("_FILE_") or
           name.upper().startswith("_ROW_TIMESTAMP")):
            return self.temp_technical_column_prefix + name
        return name

    def _generate_temp_tables(self) -> Tuple[str, str, str, int]:
        """Generate part of the future script to execute.

        Returns:
            Tuple[str, str, str]:
                tuple of queries to substitute with the table, temp table create statements and drop of them.
        """
        temp_table_queries = {}
        create_table_statements = ""
        drop_table_statements = ""
        nb_statements = 0
        for table_def in self.temp_tables:
            transformer, tables = table_def
            for (table_name, (datum, schema)) in tables.items():
                logger.info("Generating temp table %s", table_name)

                datum_literal = transformer.load(datum, schema, self._rename_technical_column)
                temp_table_create = f"CREATE TEMP TABLE {table_name} as {datum_literal};"
                temp_table_drop = f"DROP TABLE {table_name};"
                logger.debug("Generated prepend statement as :\n%s", temp_table_create)
                logger.debug("Generated append statement as :\n%s", temp_table_create)
                create_table_statements += temp_table_create + "\n"
                drop_table_statements += "\n" + temp_table_drop
                temp_table_query = self._simple_select(table_name, schema, self._rename_technical_column)
                temp_table_queries.update({table_name: temp_table_query})
                nb_statements += 1
        return (temp_table_queries, create_table_statements, drop_table_statements, nb_statements)

    @staticmethod
    def _simple_select(table_name: str, schema: List[SchemaField],
                       transform_field_name: Optional[Callable[[str], str]]) -> str:
        """Select all fields of the schema, allowing to transform the fields as well and restore its original name.

        Args:
            data_line (Dict[str, Any]): data_line which must be a dictionary since a schema is kind of record.
            schema (List[SchemaField]): schema to match the transformation with.
            transform_field_name (Optional[Callable[[str], str]]):
                function to change field name.
                Used to transform given name as the source name and not as the output name.
                Given a column _PARTITIONDATE in the schema, source name should have a name like _BQTK_PARTITIONDATE
                and output selection will be _PARTITIONDATE, as expected.
        """
        effective_transform_field_name = transform_field_name if transform_field_name else lambda x: x
        # Disabling too many statements since this is related to nested functions.
        # Function scope is only for _transform_to_literal
        # pylint: disable=R0915

        def _transform_repeated_field_to_literal(parent_path: Optional[str], schema_field: SchemaField):
            current_projection = None
            nested_result = None
            unnest_name = "ut" + str(uuid.uuid4()).replace("-", "_")
            if str.upper(schema_field.field_type) == "RECORD":
                nested_result = _transform_struct_to_literal(schema_field.fields, False, unnest_name, None)
            else:
                nested_result = unnest_name
            field_projection = _prefix_column(parent_path, effective_transform_field_name(schema_field.name))
            current_projection = f"(select array_agg({nested_result}) " \
                                 f"from unnest({field_projection}) {unnest_name}) " \
                                 f"as {schema_field.name}"
            return current_projection

        def _prefix_column(parent_path: Optional[str], name: str):
            parent_prefix = f"{parent_path}." if parent_path else ""
            return f"{parent_prefix}{name}"

        def _transform_field_to_literal(parent_path: Optional[str], schema_field: SchemaField):
            field_projection = _prefix_column(parent_path, effective_transform_field_name(schema_field.name))
            return f"{field_projection} as {schema_field.name}"

        def _transform_struct_to_literal(schema: List[SchemaField], is_root: bool,
                                         parent_path: Optional[str], key: Optional[str]):
            # Disable this too many local variable. Didn't find a way to simplify this.
            # pylint: disable=R0914
            # Disable too many branches since rework may make it less readable (dispatch of functions)
            # pylint: disable=R0912
            current_projection = []
            result = (None, None)
            schema_fields_name = [field.name for field in schema]
            for child_key in schema_fields_name:
                schema_field = next(field for field in schema if field.name == child_key)
                field_projection = None
                if str.upper(schema_field.mode) == "REPEATED":
                    field_projection = _transform_repeated_field_to_literal(parent_path, schema_field)
                elif str.upper(schema_field.field_type) == "RECORD":
                    field_path = _prefix_column(parent_path, effective_transform_field_name(child_key))
                    field_projection = _transform_struct_to_literal(schema_field.fields, False, field_path, child_key)
                else:
                    field_projection = _transform_field_to_literal(parent_path, schema_field)
                current_projection.append(field_projection)
            nested_query = ", ".join(current_projection)
            alias = f" as {effective_transform_field_name(key)}" if key else ""
            final_projection = nested_query if is_root else f"struct({nested_query}){alias}"
            result = final_projection
            return result
        query = _transform_struct_to_literal(schema, True, None, None)
        query = f"(select {query} from {table_name})"
        return query

    @staticmethod
    def _default_resource_to_kv(bq_resource: BaseBQResource) -> Tuple[str, Any]:
        key = None
        if isinstance(bq_resource, Table):
            if bq_resource.alias:
                key = bq_resource.alias
            else:
                key = f"{bq_resource.dataset.name}_{bq_resource.name}"
        else:
            key = bq_resource.name
        return key, bq_resource.fqdn()

    def __deepcopy__(self, memo) -> 'BQQueryTemplate':
        return BQQueryTemplate(
            from_=deepcopy(self.from_, memo),
            bqtk_config=deepcopy(self.bqtk_config, memo),
            location=deepcopy(self.location, memo),
            # copy is not done because bq client have non-trivial state
            # that is local and unpickleable
            bq_client=self._bq_client,
            job_config=deepcopy(self.job_config, memo),
            project=deepcopy(self.project, memo),
            interpolators=deepcopy(self.interpolators),
            global_dict=deepcopy(self.global_dict),
            temp_tables=deepcopy(self.temp_tables),
            temp_technical_column_prefix=deepcopy(self.temp_technical_column_prefix)
        )
