# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from copy import deepcopy
from typing import Optional

from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.job import WriteDisposition
from logzero import logger

from bq_test_kit.constants import DEFAULT_JOB_ID_PREFIX
from bq_test_kit.resource_loaders.package_file_loader import PackageFileLoader


class BaseDataLoader():
    """
        Base of all data loader. Currently all data loader supports only PackageFileLoader because this DSL
        relies on file load only.
    """

    def __init__(self,
                 *, table, partition: Optional[str] = None, from_: PackageFileLoader,
                 bq_client: Client, load_job_config: LoadJobConfig = LoadJobConfig()):
        """Constructor of BaseDataLoader

        Args:
            table (Table): table to load data into.
            from_ (PackageFileLoader): specifies where data is.
            bq_client (Client): instance of bigquery client to use accross the DSL.
            partition (Optional[str], optional): if you plan to load into a specific partition. Used as a decorator.
                Defaults to None.
            load_job_config (LoadJobConfig, optional): Big Query load job config.
                This is the object updated by this DSL. Defaults to LoadJobConfig().
        """
        self.load_job_config = load_job_config
        self.table = table
        self.from_ = from_
        self._bq_client = bq_client
        self.partition = partition

    def load(self):
        """Load data from the given resource loader into the specified table.

        Returns:
            [type]: [description]
        """
        with open(self.from_.absolute_path(), 'rb') as source_file:
            fqdn = self.table.fqdn()
            _partition = "$" + self.partition if self.partition else ""
            target = fqdn + _partition
            logger.info("Loading %s into %s with job config %s",
                        self.from_.absolute_path(), target, self.load_job_config)
            load_job = self._bq_client.load_table_from_file(
                source_file,
                target,
                job_id_prefix=DEFAULT_JOB_ID_PREFIX,
                location=self.table.dataset.location,
                project=self.table.dataset.project.fqdn(),
                job_config=self.load_job_config
            )
            logger.info("Job id is : %s", load_job.job_id)
            load_job.result()
        return self.table

    def ignore_unknown_values(self, ignore: bool = True):
        """Ignore extra values not represented in the table schema.

        Args:
            ignore (bool, optional): ignore value. Defaults to True.

        Returns:
            BaseDataLoader: new instance of the current data loader with ignore_unknown_values set to 'ignore'.
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.ignore_unknown_values = ignore
        return data_loader

    def overwrite(self):
        """Truncate either the partition if partition is defined or the table.

        Returns:
            BaseDataLoader: new instance of the current data loader with write_disposition set to 'truncate'.
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        return data_loader

    def append(self):
        """Append data either to the partition or to the table. If partitioned, data will be appended to the current
           partition day.

        Returns:
            BaseDataLoader: new instance of the current data loader with write_disposition set to 'append'.
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.write_disposition = WriteDisposition.WRITE_APPEND
        return data_loader

    def error_if_exists(self):
        """If data already exist in the target partition or table if it's not partitioned, then an exception is thrown.

        Returns:
            BaseDataLoader: new instance of the current data loader with ignore_unknown_values set to 'ignore'.
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.write_disposition = WriteDisposition.WRITE_EMPTY
        return data_loader

    def to_partition(self, partition: str):
        """Specify a partition where the data has to be loaded into. Used as a decorator.

        Args:
            partition (str): decorator such as 20201121

        Returns:
            BaseDataLoader: new instance of the current data loader with ignore_unknown_values set to 'ignore'.
        """
        data_loader = deepcopy(self)
        data_loader.partition = partition
        return data_loader

    def _deepcopy_base_data_loader(self, target_type, memo, **kwargs):
        return target_type(
            table=deepcopy(self.table, memo),
            from_=deepcopy(self.from_, memo),
            partition=deepcopy(self.partition, memo),
            # copy is not done because bq client have non-trivial state
            # that is local and unpickleable
            bq_client=self._bq_client,
            load_job_config=deepcopy(self.load_job_config, memo),
            **kwargs
        )

    def __deepcopy__(self, memo):
        return self._deepcopy_base_data_loader(BaseDataLoader, memo)
