# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from copy import deepcopy

from google.cloud.bigquery.table import PartitionRange, RangePartitioning
from google.cloud.bigquery.table import Table as BQTable

from bq_test_kit.bq_dsl.bq_resources.partitions.base_partition import \
    BasePartition


class Range(BasePartition):
    """Apply range partition on a resource.
    """
    def __init__(self, *, on_field: str, start: int, end: int, interval: int):
        self.field = on_field
        self.start = start
        self.end = end
        self.interval = interval

    def apply(self, bq_resource: BQTable) -> BQTable:
        new_resource = deepcopy(bq_resource)
        new_resource.range_partitioning = RangePartitioning(
            field=self.field,
            range_=PartitionRange(start=self.start,
                                  end=self.end,
                                  interval=self.interval)
        )
        return new_resource
