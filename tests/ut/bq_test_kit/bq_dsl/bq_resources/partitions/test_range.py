# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from google.cloud.bigquery.table import RangePartitioning
from google.cloud.bigquery.table import Table as BQTable

from bq_test_kit.bq_dsl.bq_resources.partitions import Range


def test_apply_range():
    table = BQTable("project.dataset.table")
    assert table.time_partitioning is None
    table = Range(on_field="f1", start=0, end=100, interval=10).apply(table)
    assert isinstance(table.range_partitioning, RangePartitioning)
    assert table.range_partitioning.field == "f1"
    assert table.range_partitioning.range_.start == 0
    assert table.range_partitioning.range_.end == 100
    assert table.range_partitioning.range_.interval == 10
