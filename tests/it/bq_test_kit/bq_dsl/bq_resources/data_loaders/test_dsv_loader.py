# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from datetime import datetime, timezone

import pytest
from google.cloud.bigquery.job import QueryJob
from google.cloud.bigquery.schema import SchemaField

from bq_test_kit import BQTestKit
from bq_test_kit.bq_dsl.bq_resources.partitions import IngestionTime
from bq_test_kit.resource_loaders.package_file_loader import PackageFileLoader


def test_csv_load(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        schema = [SchemaField("f1", field_type="STRING"), SchemaField("f2", field_type="INT64")]
        with ds.table("table_bar", schema=schema).isolate() as t:
            pfl = PackageFileLoader("tests/it/bq_test_kit/bq_dsl/bq_resources/data_loaders/resources/dummy_data.csv")
            t.dsv_loader(from_=pfl).load()
            job: QueryJob = t._bq_client.query(f"select count(*) as nb from `{t.fqdn()}`")
            rows_it = job.result()
            rows = list(rows_it)
            assert len(rows) == 1
            assert rows[0].nb == 2
            job: QueryJob = t._bq_client.query(f"select * from `{t.fqdn()}` order by f1")
            rows_it = job.result()
            rows = list(rows_it)
            rows_as_dict = [dict(row.items()) for row in rows]
            assert rows_as_dict == [{"f1": "value1", "f2": 2}, {"f1": "value3", "f2": 4}]


def test_csv_load_in_partition(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        schema = [SchemaField("f1", field_type="STRING"), SchemaField("f2", field_type="INT64")]
        with ds.table("table_bar", schema=schema).partition_by(IngestionTime()).isolate() as t:
            pfl = PackageFileLoader("tests/it/bq_test_kit/bq_dsl/bq_resources/data_loaders/resources/dummy_data.csv")
            t.dsv_loader(from_=pfl).to_partition("20201112").load()
            job: QueryJob = t._bq_client.query(f"select max(_partitiontime) as pt from `{t.fqdn()}`")
            rows_it = job.result()
            rows = list(rows_it)
            assert len(rows) == 1
            assert rows[0].pt == datetime(2020, 11, 12, 00, 00, tzinfo=timezone.utc)


def test_csv_append_in_partition(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        schema = [SchemaField("f1", field_type="STRING"), SchemaField("f2", field_type="INT64")]
        with ds.table("table_bar", schema=schema).partition_by(IngestionTime()).isolate() as t:
            pfl = PackageFileLoader("tests/it/bq_test_kit/bq_dsl/bq_resources/data_loaders/resources/dummy_data.csv")
            t.dsv_loader(from_=pfl).to_partition("20201112").load()
            t.dsv_loader(from_=pfl).to_partition("20201112").load()
            t.dsv_loader(from_=pfl).to_partition("20201112").append().load()
            job: QueryJob = t._bq_client.query(f"select count(*) as nb from `{t.fqdn()}`")
            rows_it = job.result()
            rows = list(rows_it)
            assert rows[0]["nb"] == 6


def test_csv_truncate_in_partition(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        schema = [SchemaField("f1", field_type="STRING"), SchemaField("f2", field_type="INT64")]
        with ds.table("table_bar", schema=schema).partition_by(IngestionTime()).isolate() as t:
            pfl = PackageFileLoader("tests/it/bq_test_kit/bq_dsl/bq_resources/data_loaders/resources/dummy_data.csv")
            t.dsv_loader(from_=pfl).to_partition("20201112").load()
            t.dsv_loader(from_=pfl).to_partition("20201112").overwrite().load()
            job: QueryJob = t._bq_client.query(f"select count(*) as nb from `{t.fqdn()}`")
            rows_it = job.result()
            rows = list(rows_it)
            assert rows[0]["nb"] == 2


def test_csv_error_if_exists_in_partition(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        schema = [SchemaField("f1", field_type="STRING"), SchemaField("f2", field_type="INT64")]
        with ds.table("table_bar", schema=schema).partition_by(IngestionTime()).isolate() as t:
            pfl = PackageFileLoader("tests/it/bq_test_kit/bq_dsl/bq_resources/data_loaders/resources/dummy_data.csv")
            t.dsv_loader(from_=pfl).to_partition("20201111").error_if_exists().load()
            t.dsv_loader(from_=pfl).to_partition("20201112").error_if_exists().load()
            with pytest.raises(Exception):
                t.dsv_loader(from_=pfl).to_partition("20201112").error_if_exists().load()
