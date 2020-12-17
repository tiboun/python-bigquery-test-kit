# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from copy import deepcopy
from functools import reduce
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from google.cloud.bigquery import Client
from google.cloud.bigquery.job import (QueryJob, QueryJobConfig,
                                       WriteDisposition)
from google.cloud.bigquery.query import (ArrayQueryParameter,
                                         ScalarQueryParameter,
                                         StructQueryParameter, UDFResource)
from logzero import logger

from bq_test_kit.bq_dsl.bq_query_results import BQQueryResult
from bq_test_kit.bq_dsl.bq_resources import BaseBQResource, Project, Table
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.constants import DEFAULT_JOB_ID_PREFIX
from bq_test_kit.interpolators.base_interpolator import BaseInterpolator
from bq_test_kit.resource_loaders import BaseResourceLoader


class BQQueryTemplate():
    """Query DSL which allows query to be interpolated before its execution.
    """

    def __init__(self,
                 *, from_: Union[BaseResourceLoader, str], bqtk_config: BQTestKitConfig,
                 location: Optional[str] = None, bq_client: Client,
                 job_config: QueryJobConfig = None, project: Project = None,
                 interpolators: List[BaseInterpolator] = None, global_dict: Dict[str, Any] = None) -> None:
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
        """
        self.from_ = from_
        self._bq_client = bq_client
        self.job_config = job_config if job_config else QueryJobConfig()
        self.location = location if location else bqtk_config.get_default_location()
        self.project = project
        self.interpolators = interpolators if interpolators else []
        self.bqtk_config = bqtk_config
        self.global_dict = global_dict if global_dict else {}

    def run(self) -> BQQueryResult:
        """Execute the query and return a BQQueryResult.

        Returns:
            BQQueryResult: results are stored in this object.
        """
        interpolated_query = self._interpolate()
        logger.debug("Query rendered as :\n%s", interpolated_query)
        query_job: QueryJob = self._bq_client.query(
            interpolated_query,
            job_id_prefix=DEFAULT_JOB_ID_PREFIX,
            job_config=self.job_config,
            location=self.location,
            project=self.project.fqdn() if self.project else None
        )
        logger.info("Job id is : %s", query_job.job_id)
        row_iterator = query_job.result(max_results=0 if self.job_config.destination else None)
        return BQQueryResult(row_iterator)

    def allow_large_results(self, allow: bool):
        """Allow large query results tables (legacy SQL, only)

        Args:
            allow (bool): allow or not

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with allow_large_results set.
        """
        query_template = deepcopy(self)
        query_template.job_config.allow_large_results = allow
        return query_template

    def with_destination(self, table: Table, partition: str = None):
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

    def with_query_parameters(self,
                              params: List[Union[ArrayQueryParameter, ScalarQueryParameter, StructQueryParameter]]):
        """Set query parameters when query contains parameters.

        Args:
            params (List[Union[ArrayQueryParameter, ScalarQueryParameter, StructQueryParameter]]): list of parameters.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with query parameters set.
        """
        query_template = deepcopy(self)
        query_template.job_config.query_parameters = params
        return query_template

    def with_udf_resources(self, udf_resources: List[UDFResource]):
        """Set udf resources to use along with the query.

        Args:
            udf_resources (List[UDFResource]): list of udf resources to use.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with a list of udf resources set.
        """
        query_template = deepcopy(self)
        query_template.job_config.udf_resources = udf_resources
        return query_template

    def add_udf_resource(self, udf_resource: UDFResource):
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

    def use_legacy_sql(self, use: bool = True):
        """Use legacy sql syntax instead of standard sql.

        Args:
            use (bool, optional): use legacy sql. Defaults to True.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with legacy sql usage set.
        """
        query_template = deepcopy(self)
        query_template.job_config.use_legacy_sql = use
        return query_template

    def use_query_cache(self, use: bool = True):
        """Look for the query result in the cache.

        Args:
            use (bool, optional): use query result in the cache. Defaults to True.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with cache usage set.
        """
        query_template = deepcopy(self)
        query_template.job_config.use_query_cache = use
        return query_template

    def overwrite(self):
        """Truncate destination if it exists.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with data overwrite set.
        """
        query_template = deepcopy(self)
        query_template.job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        return query_template

    def append(self):
        """Append data to the destination.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with data append set.
        """
        query_template = deepcopy(self)
        query_template.job_config.write_disposition = WriteDisposition.WRITE_APPEND
        return query_template

    def error_if_exists(self):
        """Throw error if destination has data already.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with error to thrown if data exists.
        """
        query_template = deepcopy(self)
        query_template.job_config.write_disposition = WriteDisposition.WRITE_EMPTY
        return query_template

    def with_interpolators(self, renderers: List[BaseInterpolator]):
        """Interpolators used in order to interpolate query template before its execution.

        Args:
            renderers (List[BaseInterpolator]): list of interpolators to use.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with a list of interpolators.
        """
        query_template = deepcopy(self)
        query_template.interpolators = renderers
        return query_template

    def add_interpolator(self, renderer: BaseInterpolator):
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

    def with_global_dict(self, global_dict: Dict[str, Any]):
        """Set global dictionary to use with interpolators.

        Args:
            global_dict (Dict[str, Any]): global dictionary to use with interpolators.

        Returns:
            BQQueryTemplate: a new instance of BQQueryTemplate with global dict overwritten.
        """
        query_template = deepcopy(self)
        query_template.global_dict = global_dict
        return query_template

    def update_global_dict(self, dict_resource: Union[Dict[str, Any], List[BaseBQResource]]):
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

    def update_global_dict_with_dict(self, dict_update: Dict[str, Any]):
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
                                             resource_to_kv: Callable[[BaseBQResource], Tuple[str, Any]] = None):
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

    def _interpolate(self):
        query = self.from_ if isinstance(self.from_, str) else self.from_.load()
        return reduce(lambda template, interpolator: interpolator.interpolate(template, self.global_dict),
                      self.interpolators, query)

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

    def __deepcopy__(self, memo):
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
            global_dict=deepcopy(self.global_dict)
        )
