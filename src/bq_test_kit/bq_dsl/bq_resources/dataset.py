# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from copy import deepcopy
from typing import List, Optional

from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset as BQDataset
from google.cloud.bigquery.schema import SchemaField
from logzero import logger

from bq_test_kit.bq_dsl.bq_resources.base_bq_resource import BaseBQResource
from bq_test_kit.bq_dsl.bq_resources.resource_strategy import (
    BaseResourceStrategy, CleanAfter, CleanBeforeAndKeepAfter, Noop)
from bq_test_kit.bq_dsl.bq_resources.table import Table
from bq_test_kit.bq_test_kit_config import BQTestKitConfig


class Dataset(BaseBQResource):
    """Dataset DSL which allows you to define its properties and tables.

    Args:
        BaseBQResource (BaseBQResource): Project is a BigQuery resource.
    """

    def __init__(self, name: str,
                 *, project, bq_client: Client,
                 bqtk_config: BQTestKitConfig, resource_strategy: BaseResourceStrategy = CleanAfter(),
                 isolate_with=lambda x: x.name, location: str = None, tables: List[Table] = None,
                 **create_options) -> None:
        """Constructor of Dataset

        Args:
            name (str): name of the dataset
            project (Project): project that this instance belongs to.
            bq_client (Client): instance of bigquery client to use accross the datasetL.
            bqtk_config (BQTestKitConfig): config used accross the datasetL.
            resource_strategy (BaseResourceStrategy, optional): prevent management of the dataset if it's noop.
                Defaults to CleanAfter.
            isolate_with (Callable[[Dataset], str], optional): lambda x -> str where x is itself.
                Defaults to lambda x:x.name, that is to say, no isolation.
            location (str, optional): force location for dataset. Defaults extracted from bqtk_config.
            tables (List[Table], optional): list of all tables created with the datasetL belonging to this dataset.
                Defaults to [].
            create_options: Options available in Client.create_dataset :
                - exists_ok
                - retry
                - timeout
        """
        super().__init__(name=name, bq_client=bq_client,
                         bqtk_config=bqtk_config)
        self.project = project
        self.location = None
        self.resource_strategy = resource_strategy
        self.isolate_func = isolate_with
        self.create_options = create_options
        default_location = bqtk_config.get_default_location()
        self.location = location if location else default_location
        self.tables = tables if tables else []

    def __enter__(self):
        dataset_strategy = self.resource_strategy
        dataset_strategy.before(self.delete, self.create)
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> None:
        dataset_strategy = self.resource_strategy
        dataset_strategy.after(self.delete)

    def fqdn(self) -> str:
        project_fqdn = self.project.fqdn()
        final_name = self.isolate_func(self)
        return f"{project_fqdn}.{final_name}"

    def table(self, name: str,
              *, alias: Optional[str] = None,
              schema: List[SchemaField] = None) -> Table:
        """Go down one step to the Table datasetL. Table is linked to the current dataset.

        Args:
            name (str): table name
            alias (Optional[str], optional): alias that may be used in the query template datasetL. Defaults to None.
            schema (List[SchemaField], optional): Table schema. Defaults to [].

        Returns:
            Table: Table datasetL
        """
        table = next((t for t in self.tables if t.name == name), None)
        if not table:
            dataset = deepcopy(self)
            table_schema = schema if schema else []
            table = Table(name, from_dataset=dataset, alias=alias, schema=table_schema, bq_client=self._bq_client,
                          bqtk_config=self.bqtk_config)
            dataset.tables.append(table)
        return table

    def isolate(self, *, with_=None):
        """Isolate dataset name

        Args:
            with_ (Callable[[Dataset], str], optional): function used to transform dataset name in order to isolate it.
                If isolation has to be removed, use lambda x: x.name.
                Defaults to BaseBQResource.isolate_with_context.

        Returns:
            Dataset: new instance of Dataset with isolation set.
        """
        if not with_:
            with_ = self.isolate_with_context()
        dataset = deepcopy(self)
        dataset.isolate_func = with_
        return dataset

    def noop(self):
        """Prevent management of the dataset

        Returns:
            Dataset: new instance of Dataset with noop strategy.
        """
        dataset = deepcopy(self)
        dataset.resource_strategy = Noop()
        return dataset

    def clean_and_keep(self):
        """Clean before its usage and keep after.

        Returns:
            Dataset: new instance of Dataset with CleanBeforeAndKeepAfter strategy.
        """
        dataset = deepcopy(self)
        dataset.resource_strategy = CleanBeforeAndKeepAfter()
        return dataset

    def with_resource_strategy(self, resource_strategy: BaseResourceStrategy):
        """Prevent management of the table

        Args:
            resource_strategy (BaseResourceStrategy): resource strategy to use.

        Returns:
            Table: new instance of Table with the resource_strategy
        """
        dataset = deepcopy(self)
        dataset.resource_strategy = resource_strategy
        return dataset

    def with_create_options(self, **create_options):
        """Specify create options available in Client.create_dataset:
                - exists_ok
                - retry
                - timeout

        Returns:
            Dataset: new instance of Dataset with create options.
        """
        dataset = deepcopy(self)
        dataset.create_options = create_options
        return dataset

    def with_location(self, location: str):
        """Set location of the dataset.

        Args:
            location (str): location of the dataset.

        Returns:
            Dataset: new instance of Dataset with location.
        """
        dataset = deepcopy(self)
        dataset.location = location
        return dataset

    def create(self) -> None:
        """Create dataset with the computed fqdn and the specified create options.
        """
        fqdn = self.fqdn()
        logger.info("Creating dataset %s", fqdn)
        try:
            bqdataset: BQDataset = BQDataset(fqdn)
            bqdataset.location = self.location
            self._bq_client.create_dataset(bqdataset, **self.create_options)
        except Exception:
            logger.error("Failed to create dataset %s at %s with options %s.",
                         fqdn, self.location, self.create_options)
            raise
        else:
            logger.info("Dataset %s at %s has been created.", fqdn, self.location)

    def show(self) -> BQDataset:
        """Retrieve dataset infos from BigQuery.
           Throw exceptions if dataset doesn't exist.

        Returns:
            BQDataset: BigQuery dataset infos.
        """
        fqdn = self.fqdn()
        logger.info("Deleting dataset %s", fqdn)
        return self._bq_client.get_dataset(fqdn)

    def delete(self) -> None:
        """Delete current dataset. Doesn't throw any exception when dataset doesn't exist.
           Prevent deletion of the dataset when tables still exist under it for security reason.
        """
        fqdn = self.fqdn()
        logger.info("Deleting dataset %s", fqdn)
        try:
            bqdataset: BQDataset = BQDataset(fqdn)
            self._bq_client.delete_dataset(bqdataset,
                                           delete_contents=False,
                                           not_found_ok=True)
        except Exception:
            logger.error("Failed to delete dataset %s. Please delete it yourself.", fqdn)
            raise
        else:
            logger.info("Dataset %s has been deleted.", fqdn)

    @property
    def project(self):
        """return project linked by this dataset.

        Returns:
            Project: project that this dataset belongs to.
        """
        return self._project

    @project.setter
    def project(self, project):
        """set project

        Args:
            prj (Project): Project where this dataset belongs to.
        """
        self._project = project
        self.dataset = project.dataset

    def __deepcopy__(self, memo):
        dataset = Dataset(
            deepcopy(self.name, memo),
            project=deepcopy(self.project, memo),
            # copy is not done because bq client have non-trivial state
            # that is local and unpickleable
            bq_client=self._bq_client,
            bqtk_config=deepcopy(self.bqtk_config, memo),
            resource_strategy=deepcopy(self.resource_strategy, memo),
            isolate_with=deepcopy(self.isolate_func, memo),
            location=deepcopy(self.location, memo),
            tables=deepcopy(self.tables, memo),
            **deepcopy(self.create_options, memo)
        )
        dataset.project.datasets = [dataset if self.name == pd.name else pd for pd in dataset.project.datasets]
        for table in dataset.tables:
            table.dataset = dataset
        return dataset
