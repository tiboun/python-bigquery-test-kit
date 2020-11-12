# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from copy import deepcopy
from typing import Optional

from google.cloud.bigquery.client import Client
from google.cloud.bigquery.job import LoadJobConfig, SourceFormat

from bq_test_kit.bq_dsl.bq_resources.data_loaders.base_data_loader import \
    BaseDataLoader
from bq_test_kit.bq_dsl.bq_resources.data_loaders.mixins.raw_file_loader_mixin import \
    RawFileLoaderMixin
from bq_test_kit.resource_loaders.package_file_loader import PackageFileLoader


class JsonDataLoader(BaseDataLoader, RawFileLoaderMixin):
    """Load json source data file into table
    """

    def __init__(self,
                 *, table, partition: Optional[str] = None, from_: PackageFileLoader,
                 bq_client: Client, load_job_config: LoadJobConfig = LoadJobConfig()):
        """Constructor of JsonDataLoader.

        Args:
            table (Table): table to load data into.
            from_ (PackageFileLoader): specifies where data is.
            bq_client (Client): instance of bigquery client to use accross the DSL.
            partition (Optional[str], optional): if you plan to load into a specific partition. Used as a decorator.
                Defaults to None.
            load_job_config (LoadJobConfig, optional): Big Query load job config.
                This is the object updated by this DSL. Defaults to LoadJobConfig().
        """
        _load_job_config = deepcopy(load_job_config)
        _load_job_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
        super().__init__(table=table, partition=partition, from_=from_,
                         bq_client=bq_client, load_job_config=_load_job_config)

    def __deepcopy__(self, memo):
        return self._deepcopy_base_data_loader(JsonDataLoader, memo)
