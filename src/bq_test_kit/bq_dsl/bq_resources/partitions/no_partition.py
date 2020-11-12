# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from google.cloud.bigquery.table import Table as BQTable

from bq_test_kit.bq_dsl.bq_resources.partitions.base_partition import \
    BasePartition


class NoPartition(BasePartition):
    """Act as an identity: do nothing.
    """
    def apply(self, bq_resource: BQTable) -> BQTable:
        return bq_resource
