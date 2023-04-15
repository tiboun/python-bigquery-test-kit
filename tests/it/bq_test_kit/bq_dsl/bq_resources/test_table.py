# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest
from google.api_core.exceptions import NotFound, BadRequest
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import PartitionRange, RangePartitioning

from bq_test_kit import BQTestKit
from bq_test_kit.bq_dsl.bq_resources.clustering import Clustering
from bq_test_kit.bq_dsl.bq_resources.partitions import (IngestionTime, Range,
                                                        TimeField)
from bq_test_kit.bq_dsl.bq_resources.partitions import \
    TimePartitioningType as TPT
from bq_test_kit.bq_dsl.bq_resources.partitions.time_partitionning_type import \
    TimePartitioningType
from bq_test_kit.bq_dsl.bq_resources.resource_strategy import \
    CleanBeforeAndAfter


def test_isolate(bqtk: BQTestKit, bqtk_default_context: str):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        with ds.table("table_bar").isolate() as t:
            show_res = t.show()
            assert show_res is not None
            assert show_res.table_id == f"table_bar_{bqtk_default_context}"
            assert show_res.schema == []


def test_with_schema(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        schema = [SchemaField("f1", field_type="DATETIME")]
        with ds.table("table_bar", schema=schema).isolate() as t:
            show_res = t.show()
            assert show_res is not None
            assert show_res.schema == schema


def test_noop(bqtk: BQTestKit, bqtk_default_context: str):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        with ds.table(f"table_{bqtk_default_context}").noop() as t:
            with pytest.raises(NotFound):
                t.show()


def test_keep(bqtk: BQTestKit, bqtk_default_context: str):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        table = ds.table(f"table_{bqtk_default_context}").clean_and_keep()
        with table:
            assert table.show() is not None
        assert table.show() is not None
        table.delete()


def test_clean_always(bqtk: BQTestKit, bqtk_default_context: str):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        table = ds.table(f"table_{bqtk_default_context}").with_resource_strategy(CleanBeforeAndAfter())
        table.create()
        assert table.show() is not None
        with table:
            assert table.show() is not None
        with pytest.raises(NotFound):
            table.show()


def test_change_create_options(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        with ds.table("table_bar").isolate() as t:
            assert t.show() is not None
            create_options = {"exists_ok": True}
            with t.with_create_options(**create_options) as t2:
                show_res = t2.show()
                assert show_res is not None
            with pytest.raises(NotFound):
                t.show()


def test_change_partition_by_ingestion_time(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        with ds.table("table_bar"). \
                partition_by(IngestionTime(type_=TimePartitioningType.MONTH)).isolate() as t:
            show_res = t.show()
            assert show_res is not None
            assert show_res.time_partitioning.type_ == TPT.MONTH.value
            assert show_res.range_partitioning is None
            assert show_res.time_partitioning.field is None


def test_change_partition_by_day_time_field(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        table_bar_schema = [SchemaField("my_time", "TIMESTAMP")]
        with ds.table("table_bar"). \
                with_schema(from_=table_bar_schema). \
                partition_by(TimeField("my_time", type_=TPT.DAY)). \
                isolate() as t:
            show_res = t.show()
            assert show_res is not None
            assert show_res.time_partitioning.type_ == TPT.DAY.value
            assert show_res.range_partitioning is None
            assert show_res.time_partitioning.field == "my_time"


def test_change_partition_by_hour_time_field(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        table_bar_schema = [SchemaField("my_time", "TIMESTAMP")]
        with ds.table("table_bar"). \
                with_schema(from_=table_bar_schema). \
                partition_by(TimeField("my_time", type_=TPT.HOUR)). \
                isolate() as t:
            show_res = t.show()
            assert show_res is not None
            assert show_res.time_partitioning.type_ == TPT.HOUR.value
            assert show_res.range_partitioning is None
            assert show_res.time_partitioning.field == "my_time"


def test_change_partition_by_month_time_field(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        table_bar_schema = [SchemaField("my_time", "TIMESTAMP")]
        with ds.table("table_bar"). \
                with_schema(from_=table_bar_schema). \
                partition_by(TimeField("my_time", type_=TPT.MONTH)). \
                isolate() as t:
            show_res = t.show()
            assert show_res is not None
            assert show_res.time_partitioning.type_ == TPT.MONTH.value
            assert show_res.range_partitioning is None
            assert show_res.time_partitioning.field == "my_time"


def test_change_partition_by_year_time_field(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        table_bar_schema = [SchemaField("my_time", "TIMESTAMP")]
        with ds.table("table_bar"). \
                with_schema(from_=table_bar_schema). \
                partition_by(TimeField("my_time", type_=TPT.YEAR)). \
                isolate() as t:
            show_res = t.show()
            assert show_res is not None
            assert show_res.time_partitioning.type_ == TPT.YEAR.value
            assert show_res.range_partitioning is None
            assert show_res.time_partitioning.field == "my_time"


def test_change_partition_by_invalid_time_field(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        with pytest.raises(Exception):
            ds.table("table_bar"). \
                partition_by(TimeField("my_time", type_=TPT.YEAR)). \
                isolate().create()


def test_change_partition_by_range(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        table_bar_schema = [SchemaField("my_int", "int64")]
        with ds.table("table_bar"). \
                with_schema(from_=table_bar_schema). \
                partition_by(Range(on_field="my_int",
                                   start=0,
                                   end=10000,
                                   interval=10)). \
                isolate() as t:
            show_res = t.show()
            assert show_res is not None
            assert show_res.time_partitioning is None
            expected_range = RangePartitioning(field='my_int', range_=PartitionRange(end=10000, interval=10, start=0))
            assert show_res.range_partitioning == expected_range


def test_change_partition_by_invalid_field_range(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        with pytest.raises(Exception):
            ds.table("table_bar"). \
                partition_by(Range(on_field="my_int",
                                   start=0,
                                   end=10000,
                                   interval=10)). \
                isolate().create()


def test_clustering(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        table_bar_schema = [SchemaField("user_id", "STRING"), SchemaField("geo", "GEOGRAPHY")]
        with ds.table("table_bar"). \
                with_schema(from_=table_bar_schema). \
                cluster_by(Clustering("user_id", "geo")). \
                isolate() as t:
            show_res = t.show()
            assert show_res is not None
            assert show_res.clustering_fields == ["user_id", "geo"]


def test_clustering_missing_field(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        table_bar_schema = [SchemaField("user_id", "STRING"), SchemaField("geo", "GEOGRAPHY")]
        with pytest.raises(BadRequest):
            ds.table("table_bar"). \
                with_schema(from_=table_bar_schema). \
                cluster_by(Clustering("missing_field")). \
                isolate().create()


def test_clustering_wrong_field_type(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        table_bar_schema = [SchemaField("user_id", "STRING"), SchemaField("geo", "GEOGRAPHY"),
                            SchemaField("weird_field", "RECORD")]
        with pytest.raises(BadRequest):
            ds.table("table_bar"). \
                with_schema(from_=table_bar_schema). \
                cluster_by(Clustering("weird_field")). \
                isolate().create()
