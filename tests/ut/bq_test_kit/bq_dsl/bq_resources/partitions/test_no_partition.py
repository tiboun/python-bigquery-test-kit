# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from google.cloud.bigquery.table import Table as BQTable

from bq_test_kit.bq_dsl.bq_resources.partitions import NoPartition


def test_apply_no_partition():
    table = BQTable("project.dataset.table")
    assert table.time_partitioning is None
    table_applied = NoPartition().apply(table)
    assert table == table_applied
