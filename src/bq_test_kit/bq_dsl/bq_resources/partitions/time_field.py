# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from copy import deepcopy

from google.cloud.bigquery.table import Table as BQTable
from google.cloud.bigquery.table import TimePartitioning

from bq_test_kit.bq_dsl.bq_resources.partitions.base_partition import \
    BasePartition
from bq_test_kit.bq_dsl.bq_resources.partitions.time_partitionning_type import \
    TimePartitioningType


class TimeField(BasePartition):
    """Use a time field as the way to partition data.
    """

    def __init__(self, field: str, *, type_: TimePartitioningType = TimePartitioningType.DAY) -> None:
        """
        Args:
            field (str): field name of type datetime available in the schema
            type_ (TimePartitioningType): Choose the precision of time partitions. Default to DAY
        """
        self.field = field
        self.type_ = type_

    def apply(self, bq_resource: BQTable) -> BQTable:
        new_resource = deepcopy(bq_resource)
        new_resource.time_partitioning = TimePartitioning(type_=self.type_.value, field=self.field)
        return new_resource
