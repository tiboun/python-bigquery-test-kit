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


class IngestionTime(BasePartition):
    """Apply ingestion time partition on resource
    """
    def __init__(self, *, type_: TimePartitioningType = TimePartitioningType.DAY) -> None:
        self.type_ = type_

    def apply(self, bq_resource: BQTable) -> BQTable:
        new_resource = deepcopy(bq_resource)
        new_resource.time_partitioning = TimePartitioning(type_=self.type_.value)
        return new_resource
