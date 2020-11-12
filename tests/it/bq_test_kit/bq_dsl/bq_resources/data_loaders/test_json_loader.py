# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from datetime import datetime, timezone

import pytest
from google.cloud.bigquery.job import QueryJob
from google.cloud.bigquery.schema import SchemaField

from bq_test_kit import BQTestKit
from bq_test_kit.bq_dsl.bq_resources.partitions.ingestion_time import \
    IngestionTime
from bq_test_kit.resource_loaders.package_file_loader import PackageFileLoader

json_schema = [
                SchemaField("f1", field_type="STRING"),
                SchemaField("struct_f2", field_type="RECORD", fields=[
                    SchemaField("f2_1", field_type="INT64")
                ]),
                SchemaField("array_f3", field_type="RECORD", mode="REPEATED", fields=[
                    SchemaField("f3_1", field_type="DATETIME")
                ])
              ]


def test_json_load(bqtk: BQTestKit):

    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        with ds.table("table_bar", schema=json_schema).isolate() as t:
            pfl = PackageFileLoader("tests/it/bq_test_kit/bq_dsl/bq_resources/data_loaders/resources/dummy_data.json")
            t.json_loader(from_=pfl).load()
            job: QueryJob = t._bq_client.query(f"select count(*) as nb from `{t.fqdn()}`")
            rows_it = job.result()
            rows = list(rows_it)
            assert len(rows) == 1
            assert rows[0].nb == 2
            job: QueryJob = t._bq_client.query(f"select * from `{t.fqdn()}` order by f1")
            rows_it = job.result()
            rows = list(rows_it)
            rows_as_dict = [dict(row.items()) for row in rows]
            assert rows_as_dict == [
                                        {
                                            "f1": "value1",
                                            "struct_f2": {
                                                "f2_1": 1
                                            },
                                            "array_f3": [
                                                {
                                                    "f3_1": datetime(2020, 10, 21, 10, 0)
                                                },
                                                {
                                                    "f3_1": datetime(2020, 10, 21, 11, 0)
                                                }
                                            ]
                                        },
                                        {
                                            "f1": "value2",
                                            "struct_f2": {
                                                "f2_1": 2
                                            },
                                            "array_f3": [
                                                {
                                                    "f3_1": datetime(2020, 10, 21, 12, 0)
                                                },
                                                {
                                                    "f3_1": datetime(2020, 10, 21, 13, 0)
                                                }
                                            ]
                                        }
                                    ]


def test_json_load_in_partition(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        with ds.table("table_bar", schema=json_schema).partition_by(IngestionTime()).isolate() as t:
            pfl = PackageFileLoader("tests/it/bq_test_kit/bq_dsl/bq_resources/data_loaders/resources/dummy_data.json")
            t.json_loader(from_=pfl).to_partition("20201112").load()
            job: QueryJob = t._bq_client.query(f"select max(_partitiontime) as pt from `{t.fqdn()}`")
            rows_it = job.result()
            rows = list(rows_it)
            assert len(rows) == 1
            assert rows[0].pt == datetime(2020, 11, 12, 00, 00, tzinfo=timezone.utc)


def test_json_append_in_partition(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        with ds.table("table_bar", schema=json_schema).partition_by(IngestionTime()).isolate() as t:
            pfl = PackageFileLoader("tests/it/bq_test_kit/bq_dsl/bq_resources/data_loaders/resources/dummy_data.json")
            t.json_loader(from_=pfl).to_partition("20201112").load()
            t.json_loader(from_=pfl).to_partition("20201112").load()
            t.json_loader(from_=pfl).to_partition("20201112").append().load()
            job: QueryJob = t._bq_client.query(f"select count(*) as nb from `{t.fqdn()}`")
            rows_it = job.result()
            rows = list(rows_it)
            assert rows[0]["nb"] == 6


def test_json_truncate_in_partition(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        with ds.table("table_bar", schema=json_schema).partition_by(IngestionTime()).isolate() as t:
            pfl = PackageFileLoader("tests/it/bq_test_kit/bq_dsl/bq_resources/data_loaders/resources/dummy_data.json")
            t.json_loader(from_=pfl).to_partition("20201112").load()
            t.json_loader(from_=pfl).to_partition("20201112").overwrite().load()
            job: QueryJob = t._bq_client.query(f"select count(*) as nb from `{t.fqdn()}`")
            rows_it = job.result()
            rows = list(rows_it)
            assert rows[0]["nb"] == 2


def test_json_error_if_exists_in_partition(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        with ds.table("table_bar", schema=json_schema).partition_by(IngestionTime()).isolate() as t:
            pfl = PackageFileLoader("tests/it/bq_test_kit/bq_dsl/bq_resources/data_loaders/resources/dummy_data.json")
            t.json_loader(from_=pfl).to_partition("20201111").error_if_exists().load()
            t.json_loader(from_=pfl).to_partition("20201112").error_if_exists().load()
            with pytest.raises(Exception):
                t.json_loader(from_=pfl).to_partition("20201112").error_if_exists().load()
