# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from google.cloud.bigquery.table import Table as BQTable
from google.cloud.bigquery.table import TimePartitioning

from bq_test_kit.bq_dsl.bq_resources.partitions import IngestionTime
from bq_test_kit.bq_dsl.bq_resources.partitions.time_partitionning_type import \
    TimePartitioningType


def test_apply_ingestion_time():
    table = BQTable("project.dataset.table")
    assert table.time_partitioning is None
    table = IngestionTime().apply(table)
    assert isinstance(table.time_partitioning, TimePartitioning)
    assert table.time_partitioning.field is None
    assert table.time_partitioning.type_ == TimePartitioningType.DAY.value
