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


class DsvDataLoader(BaseDataLoader, RawFileLoaderMixin):
    """Loader of Delimiter-Seperated Value data. By default, it's CSV.
    """

    def __init__(self,
                 *, table, partition: Optional[str] = None, from_: PackageFileLoader,
                 bq_client: Client, load_job_config: LoadJobConfig = LoadJobConfig()):
        """Constructor of DsvDataLoader.

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
        _load_job_config.source_format = SourceFormat.CSV
        super().__init__(table=table, partition=partition, from_=from_,
                         bq_client=bq_client, load_job_config=_load_job_config)

    def allow_jagged_rows(self, allow: bool = True):
        """Allow missing trailing optional columns.

        Args:
            allow (bool, optional): allow or not jagged rows. Defaults to True.

        Returns:
            DsvDataLoader: new instance of DsvDataLoader with allow_jagged_rows set to 'allow'.
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.allow_jagged_rows = allow
        return data_loader

    def allow_quoted_newlines(self, allow: bool = True):
        """Allow quoted data containing newline characters.

        Args:
            allow (bool, optional): allow or not jagged rows. Defaults to True.

        Returns:
            DsvDataLoader: new instance of DsvDataLoader with allow_quoted_newlines set to 'allow'.
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.allow_quoted_newlines = allow
        return data_loader

    def with_field_delimiter(self, delimiter: str):
        """The field's separator.

        Args:
            delimiter (str): delimiter to use.

        Returns:
            DsvDataLoader: new instance of DsvDataLoader with updated field_delimiter.
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.field_delimiter = delimiter
        return data_loader

    def with_null_marker(self, marker: str):
        """Specifies what represent a null value.

        Args:
            marker (str): null value marker.

        Returns:
            DsvDataLoader: new instance of DsvDataLoader with updated null marker.
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.null_marker = marker
        return data_loader

    def with_quote_character(self, char: str):
        """Character used to quote data sections

        Args:
            char (str): a character.

        Returns:
            DsvDataLoader: new instance of DsvDataLoader with updated quote character.
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.quote_character = char
        return data_loader

    def skip_leading_rows(self, nb_lines: int):
        """Number of rows to skip from the beginning of the file.

        Args:
            nb_lines (int): number of lines

        Returns:
            DsvDataLoader: new instance of DsvDataLoader with updated leading rows to skip.
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.skip_leading_rows = nb_lines
        return data_loader

    def __deepcopy__(self, memo):
        return self._deepcopy_base_data_loader(DsvDataLoader, memo)
