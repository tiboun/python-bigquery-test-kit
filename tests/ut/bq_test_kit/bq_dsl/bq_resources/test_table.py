# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from json.decoder import JSONDecodeError

import pytest
from google.cloud.bigquery.schema import SchemaField

from bq_test_kit.bq_dsl import Dataset, Project, Table
from bq_test_kit.bq_dsl.bq_resources.data_loaders import (DsvDataLoader,
                                                          JsonDataLoader)
from bq_test_kit.bq_dsl.bq_resources.partitions import (BasePartition,
                                                        IngestionTime,
                                                        NoPartition, Range,
                                                        TimeField)
from bq_test_kit.bq_dsl.bq_resources.resource_strategy import (
    CleanAfter, CleanBeforeAndAfter, CleanBeforeAndKeepAfter, Noop)
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.constants import DEFAULT_LOCATION
from bq_test_kit.exceptions import InvalidInstanceException
from bq_test_kit.resource_loaders.package_file_loader import PackageFileLoader


def test_default_constructor():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    assert table.dataset.project.name == "test_project"
    assert table.dataset.name == "dataset_foo"
    assert table.name == "table_bar"
    assert isinstance(table.resource_strategy, CleanAfter)
    assert table.alias is None
    assert table.isolate_func(table) == "table_bar"
    assert table.create_options == {}
    assert table.schema == []
    assert isinstance(table.partition_type, NoPartition)


def test_full_constructor():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    schema = [SchemaField("f1", field_type="INT64")]
    table = Table("table_bar", from_dataset=ds, alias="bar", bq_client=None,
                  bqtk_config=conf, resource_strategy=Noop(),
                  isolate_with=lambda x: x.name + "bar",
                  partition_type=IngestionTime(),
                  schema=schema, exists_ok=True)
    assert table.dataset.project.name == "test_project"
    assert table.dataset.name == "dataset_foo"
    assert table.name == "table_bar"
    assert isinstance(table.resource_strategy, Noop)
    assert table.alias == "bar"
    assert table.isolate_func is not None
    assert table.isolate_func(table) == "table_barbar"
    assert len(table.create_options) == 1
    assert isinstance(table.partition_type, IngestionTime)
    assert table.schema == schema


def test_change_noop():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    table = table.noop()
    assert isinstance(table.resource_strategy, Noop)


def test_change_clean_and_keep():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    table = table.clean_and_keep()
    assert isinstance(table.resource_strategy, CleanBeforeAndKeepAfter)


def test_change_clean_always():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    table = table.with_resource_strategy(CleanBeforeAndAfter())
    assert isinstance(table.resource_strategy, CleanBeforeAndAfter)


def test_change_isolate():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    }).with_test_context("context")
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    table = table.isolate()
    assert table.isolate_func(table) == f"{table.name}_context"
    assert table.fqdn() == (f"{table.dataset.project.name}."
                            f"{table.dataset.name}."
                            f"{table.name}_context")


def test_change_create_options():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    }).with_test_context("context")
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    create_options = {"exists_ok": True, "timeout": 10}
    table = table.with_create_options(**create_options)
    assert table.create_options == create_options
    table = table.with_create_options()
    assert table.create_options == {}


def test_change_alias():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    table = table.with_alias("bar")
    assert table.alias == "bar"
    table = table.with_alias(None)
    assert table.alias is None


def test_change_partition_by():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    table = table.partition_by(Range(on_field="zoo",
                                     start=0,
                                     end=10000,
                                     interval=10))
    assert isinstance(table.partition_type, Range)
    assert table.partition_type.field == "zoo"
    assert table.partition_type.start == 0
    assert table.partition_type.end == 10000
    assert table.partition_type.interval == 10


def test_invalid_change_partition_by():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    with pytest.raises(Exception):
        table.partition_by("zoo")


def test_change_schema():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    table = table.with_schema(from_="""
        [
            {
                "description": "field 1",
                "mode": "REQUIRED",
                "name": "f1",
                "type": "STRING"
            }
        ]""")
    assert table.schema == [SchemaField("f1",
                                        field_type="STRING",
                                        mode="REQUIRED",
                                        description="field 1")]


def test_invalid_change_schema():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    with pytest.raises(InvalidInstanceException):
        table.with_schema(from_=[1, 2, 3])
    with pytest.raises(InvalidInstanceException):
        table.with_schema(from_=1)


def test_invalid_json_schema():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    with pytest.raises(JSONDecodeError):
        table.with_schema(from_="this is not a json")


def test_invalid_partition_impl():
    class InvalidPartition(BasePartition):
        pass
    with pytest.raises(NotImplementedError):
        InvalidPartition().apply(None)


def test_csv_loader():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    pfl = PackageFileLoader("tests/ut/bq_test_kit/resource_loaders/resources/package_file_test_resource.txt")
    assert isinstance(table.dsv_loader(from_=pfl), DsvDataLoader)


def test_json_loader():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", from_dataset=ds, bq_client=None, bqtk_config=conf)
    pfl = PackageFileLoader("tests/ut/bq_test_kit/resource_loaders/resources/package_file_test_resource.txt")
    assert isinstance(table.json_loader(from_=pfl), JsonDataLoader)


def test_table_dsl():
    """
        Check if deepcopy works well for table. Closes issue #2.
    """
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None,
                      bqtk_config=conf)
    project = project \
        .dataset("dataset_foo").isolate() \
        .table("table_foo") \
        .partition_by(IngestionTime()) \
        .table("table_bar") \
        .partition_by(TimeField("foo_date")) \
        .table("table_foobar") \
        .partition_by(IngestionTime()) \
        .project.dataset("dataset_bar").isolate() \
        .table("table_zoo") \
        .partition_by(IngestionTime()) \
        .project
    assert [dataset.name for dataset in project.datasets] == ["dataset_foo", "dataset_bar"]
    dataset_foo = project.dataset("dataset_foo")
    assert dataset_foo.name == "dataset_foo"
    assert [table.name for table in dataset_foo.tables] == ["table_foo", "table_bar", "table_foobar"]
    assert isinstance(dataset_foo.table("table_foo").partition_type, IngestionTime)
    assert isinstance(dataset_foo.table("table_bar").partition_type, TimeField)
    assert isinstance(dataset_foo.table("table_foobar").partition_type, IngestionTime)
    dataset_bar = project.dataset("dataset_bar")
    assert [table.name for table in dataset_bar.tables] == ["table_zoo"]
    assert isinstance(dataset_bar.table("table_zoo").partition_type, IngestionTime)
