# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from copy import deepcopy
from typing import List, Optional, Union

from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.table import Table as BQTable
from logzero import logger

from bq_test_kit.bq_dsl.bq_resources.base_bq_resource import BaseBQResource
from bq_test_kit.bq_dsl.bq_resources.data_loaders import (DsvDataLoader,
                                                          JsonDataLoader)
from bq_test_kit.bq_dsl.bq_resources.partitions import (BasePartition,
                                                        NoPartition)
from bq_test_kit.bq_dsl.bq_resources.resource_strategy import (
    BaseResourceStrategy, CleanAfter, CleanBeforeAndKeepAfter, Noop)
from bq_test_kit.bq_dsl.schema_mixin import SchemaMixin
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.exceptions import InvalidInstanceException
from bq_test_kit.resource_loaders import BaseResourceLoader, PackageFileLoader


class Table(BaseBQResource, SchemaMixin):
    """Table DSL which allows you to define its properties and access data loader DSL.
    """

    def __init__(self, name: str,
                 *, from_dataset, alias: Optional[str] = None,
                 resource_strategy: BaseResourceStrategy = CleanAfter(), bq_client: Client,
                 bqtk_config: BQTestKitConfig,
                 isolate_with=lambda x: x.name,
                 partition_type: BasePartition = NoPartition(),
                 schema: Union[BaseResourceLoader, str, List[SchemaField]] = None,
                 **create_options) -> None:
        """Constructor of Table

        Args:
            name (str): name of the table
            dataset (Dataset): dataset that this instance belongs to.
            alias: (Optional[str], optional) : alias of the table used only as variable name with query template.
            bq_client (Client): instance of bigquery client to use accross the datasetL.
            bqtk_config (BQTestKitConfig): config used accross the dataset DSL.
            resource_strategy (BaseResourceStrategy, optional):
                resource management strategy to use.
                Defaults to CleanAfter.
            isolate_with (Callable[[Dataset], str], optional): lambda x -> str where x is itself.
                Defaults to lambda x:x.name, that is to say, no isolation.
            partition_type (BasePartition): Kind of partition for the table. Default to NoPartition().
            schema (Union[BaseResourceLoader, str, List[SchemaField]], optional):
                Schema of the table. May be either :
                    - a string which contains BigQuery json schema
                    - a resource which contains BigQuery json schema
                    - a list of SchemaField
                Defaults to [].
            create_options: Options available in Client.create_dataset :
                - exists_ok
                - retry
                - timeout
        """
        super().__init__(name=name, bq_client=bq_client,
                         bqtk_config=bqtk_config)
        self.dataset = from_dataset
        self.alias = alias
        self.isolate_func = isolate_with
        self.create_options = create_options
        self.resource_strategy = resource_strategy
        self.partition_type = partition_type
        self.schema = schema if schema else []

    def __enter__(self):
        table_strategy = self.resource_strategy
        table_strategy.before(self.delete, self.create)
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> None:
        table_strategy = self.resource_strategy
        table_strategy.after(self.delete)

    def fqdn(self) -> str:
        dataset_fqdn = self.dataset.fqdn()
        final_name = self.isolate_func(self)
        return f"{dataset_fqdn}.{final_name}"

    def with_alias(self, alias: str):
        """Define alias of the table. Used in query template.

        Args:
            alias (str): alias used as key.

        Returns:
            Table: new instance of Table with alias set.
        """
        table = deepcopy(self)
        table.alias = alias
        return table

    def isolate(self, *, with_=None):
        """Isolate dataset name

        Args:
            with_ (Callable[[Table], str], optional): function used to transform table name in order to isolate it.
                If isolation has to be removed, use lambda x: x.name.
                Defaults to BaseBQResource.isolate_with_context.

        Returns:
            Table: new instance of Table with isolation set.
        """
        if not with_:
            with_ = self.isolate_with_context()
        table = deepcopy(self)
        table.isolate_func = with_
        return table

    def noop(self):
        """Prevent management of the table

        Returns:
            Table: new instance of Table with noop strategy.
        """
        table = deepcopy(self)
        table.resource_strategy = Noop()
        return table

    def clean_and_keep(self):
        """Clean before its usage and keep after.

        Returns:
            Table: new instance of Table with CleanBeforeAndKeepAfter strategy.
        """
        table = deepcopy(self)
        table.resource_strategy = CleanBeforeAndKeepAfter()
        return table

    def with_resource_strategy(self, resource_strategy: BaseResourceStrategy):
        """Prevent management of the table

        Args:
            resource_strategy (BaseResourceStrategy): resource strategy to use.

        Returns:
            Table: new instance of Table with the resource_strategy
        """
        table = deepcopy(self)
        table.resource_strategy = resource_strategy
        return table

    def with_create_options(self, **create_options):
        """Specify create options available in Client.create_table:
                - exists_ok
                - retry
                - timeout

        Returns:
            Table: new instance of Table with create options.
        """
        table = deepcopy(self)
        table.create_options = create_options
        return table

    def partition_by(self, partition_type: BasePartition):
        """Specify how the table should be partitioned.

        Args:
            partition_type (BasePartition): define how the table is partitioned.

        Returns:
            Table: new instance of Table with partition strategy set.
        """
        if not isinstance(partition_type, BasePartition):
            raise InvalidInstanceException(type(partition_type),
                                           expected_instances=[BasePartition])
        table = deepcopy(self)
        table.partition_type = partition_type
        return table

    def with_schema(self, *, from_: Union[BaseResourceLoader, str, List[SchemaField]]):
        """Define the schema of the current table.

        Args:
            from_ (Union[BaseResourceLoader, str, List[SchemaField]]):
                Schema of the table. May be either :
                    - a string which contains BigQuery json schema
                    - a resource which contains BigQuery json schema
                    - a list of SchemaField

        Returns:
            Table: new instance of Table with schema set.
        """
        table = deepcopy(self)
        table.schema = from_
        return table

    def dsv_loader(self, *, from_: PackageFileLoader):
        """Go down one step to the Data Loader DSL with DSV file. Default to CSV File loader.

        Args:
            from_ (PackageFileLoader): specifies where data is.

        Returns:
            DsvDataLoader: Dsv Loader DSL
        """
        return DsvDataLoader(table=self, from_=from_, bq_client=self._bq_client).skip_leading_rows(1)

    def json_loader(self, *, from_: PackageFileLoader):
        """Go down one step to the Data Loader DSL with JSON file.

        Args:
            from_ (PackageFileLoader): specifies where data is.

        Returns:
            JsonDataLoader: Json Loader DSL
        """
        return JsonDataLoader(table=self, from_=from_, bq_client=self._bq_client)

    @property
    def schema(self):
        """Schema of the table.

        Returns:
            List[SchemaField]: list of all fields of the table.
        """
        return self._schema

    @schema.setter
    def schema(self, from_: Union[BaseResourceLoader, str, List[SchemaField]]):
        self._schema = self.to_schema_field_list(from_)
        return self

    def create(self) -> None:
        """Create table with the computed fqdn and the specified create options.
        """
        fqdn = self.fqdn()
        logger.info("Creating table %s", fqdn)
        try:
            bqtable: BQTable = BQTable(fqdn, schema=self.schema)
            bqtable = self.partition_type.apply(bqtable)
            self._bq_client.create_table(bqtable, **self.create_options)
        except Exception:
            logger.error("Failed to create table %s with options %s.", fqdn, self.create_options)
            raise
        else:
            logger.info("Table %s has been created.", fqdn)

    def show(self) -> BQTable:
        """Retrieve table infos from BigQuery.
           Throw exceptions if table doesn't exist.

        Returns:
            BQTable: BigQuery table infos.
        """
        fqdn = self.fqdn()
        logger.info("Deleting dataset %s", fqdn)
        return self._bq_client.get_table(fqdn)

    def delete(self) -> None:
        """Delete current table. Doesn't throw any exception when table doesn't exist.
        """
        fqdn = self.fqdn()
        logger.info("Deleting table %s", fqdn)
        try:
            bqtable: BQTable = BQTable(fqdn)
            self._bq_client.delete_table(bqtable, not_found_ok=True)
        except Exception:
            logger.error("Failed to delete table %s. Please delete it yourself.", fqdn)
            raise
        else:
            logger.info("Table %s has been deleted.", fqdn)

    @property
    def dataset(self):
        """
        Returns:
            Dataset: dataset this table belongs to.
        """
        return self._dataset

    @dataset.setter
    def dataset(self, target_dataset):
        """Set dataset where this table belongs to and update project property as well as table function.
        Table function allows to create multiple table inside a dataset fluently.

        Args:
            dataset (Dataset): dataset where this table belongs to.
        """
        self._dataset = target_dataset
        self.project = target_dataset.project
        self.table = target_dataset.table

    def __deepcopy__(self, memo):
        table = Table(
            deepcopy(self.name, memo),
            from_dataset=deepcopy(self.dataset, memo),
            alias=deepcopy(self.alias, memo),
            # copy is not done because bq client have non-trivial state
            # that is local and unpickleable
            bq_client=self._bq_client,
            bqtk_config=deepcopy(self.bqtk_config, memo),
            resource_strategy=deepcopy(self.resource_strategy, memo),
            isolate_with=deepcopy(self.isolate_func, memo),
            partition_type=deepcopy(self.partition_type),
            schema=deepcopy(self.schema),
            **deepcopy(self.create_options, memo)
        )
        table.dataset.tables = [table if self.name == t.name else t for t in table.dataset.tables]
        return table
