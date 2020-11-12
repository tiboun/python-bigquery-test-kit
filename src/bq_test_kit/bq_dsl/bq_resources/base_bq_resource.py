# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from google.cloud.bigquery.client import Client

from bq_test_kit.bq_test_kit_config import BQTestKitConfig


class BaseBQResource():
    """ Base of all BigQuery resource
    """
    def __init__(self,
                 *, name: str, bq_client: Client,
                 bqtk_config: BQTestKitConfig) -> None:
        self._bq_client = bq_client
        self.bqtk_config = bqtk_config
        self.name = name

    @staticmethod
    def isolate_with_context():
        """

        Returns:
            lambda: lambda that take a BaseBQResource and return it's name concatenated with the context.
        """
        return lambda x:  x.name + '_' + x.bqtk_config.get_test_context()

    def fqdn(self) -> str:
        """
        Raises:
            NotImplementedError: Each Big Query reousrce must defined their fqdn otherwise it may throws this.

        Returns:
            str: valid fully qualified name used in queries of the resource.
        """
        raise NotImplementedError("Each bigquery resource should have"
                                  " a Fully Qualified Name."
                                  " Please implement fqdn method.")
