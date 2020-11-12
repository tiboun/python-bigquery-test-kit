# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from bq_test_kit.bq_dsl import Dataset, Project
from bq_test_kit.bq_dsl.bq_resources.resource_strategy import (
    CleanAfter, CleanBeforeAndAfter, CleanBeforeAndKeepAfter, Noop)
from bq_test_kit.bq_dsl.bq_resources.table import Table
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.constants import DEFAULT_LOCATION


def test_default_constructor():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None,
                      bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    assert ds.project.name == "test_project"
    assert ds.name == "dataset_foo"
    assert isinstance(ds.resource_strategy, CleanAfter)
    assert ds.isolate_func(ds) == "dataset_foo"
    assert ds.location == "EU"
    assert ds.create_options == {}
    assert ds.fqdn() == f"{ds.project.name}.{ds.name}"


def test_table_dsl():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None,
                      bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    project.datasets = [ds]
    t = ds.table("table_foo")
    assert isinstance(t, Table)
    assert t.dataset.table("table_foo") == t
    assert t.dataset.name == "dataset_foo"
    assert t.dataset.project.name == "test_project"
    assert t.dataset.project == t.project
    assert t.dataset == t.dataset.project.dataset("dataset_foo")
    t = t.dataset.table("table_bar")
    assert len(t.dataset.tables) == 2
    assert t.dataset.tables[0].dataset == t.dataset.tables[1].dataset
    assert t.dataset.tables[0].name == "table_foo"
    assert t.dataset.tables[1].name == "table_bar"
    assert t.dataset.name == "dataset_foo"
    assert t.dataset.project.name == "test_project"
    assert t.dataset.project == t.project
    t = t.table("table_foobar")
    assert len(t.dataset.tables) == 3
    assert t.dataset.tables[0].dataset == t.dataset.tables[1].dataset == t.dataset.tables[2].dataset
    assert t.dataset.tables[0].name == "table_foo"
    assert t.dataset.tables[1].name == "table_bar"
    assert t.dataset.tables[2].name == "table_foobar"
    assert t.dataset.name == "dataset_foo"
    assert t.dataset.project.name == "test_project"
    assert t.dataset.project == t.project


def test_full_constructor():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None,
                      bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf, resource_strategy=Noop(),
                 isolate_with=lambda x: x.name + 'bar', location="US",
                 exists_ok=True)
    assert ds.project.name == "test_project"
    assert ds.name == "dataset_foo"
    assert isinstance(ds.resource_strategy, Noop)
    assert ds.isolate_func is not None
    assert ds.isolate_func(ds) == ds.name + 'bar'
    assert ds.location == "US"
    assert len(ds.create_options) == 1


def test_change_noop():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None,
                      bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    ds = ds.noop()
    assert ds.project.name == "test_project"
    assert ds.name == "dataset_foo"
    assert ds.isolate_func(ds) == "dataset_foo"
    assert ds.location == "EU"
    assert ds.create_options == {}
    assert isinstance(ds.resource_strategy, Noop)


def test_change_clean_and_keep():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None,
                      bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    ds = ds.clean_and_keep()
    assert ds.project.name == "test_project"
    assert ds.name == "dataset_foo"
    assert ds.isolate_func(ds) == "dataset_foo"
    assert ds.location == "EU"
    assert ds.create_options == {}
    assert isinstance(ds.resource_strategy, CleanBeforeAndKeepAfter)


def test_change_resource_strategy():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None,
                      bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    ds = ds.with_resource_strategy(CleanBeforeAndAfter())
    assert ds.project.name == "test_project"
    assert ds.name == "dataset_foo"
    assert ds.isolate_func(ds) == "dataset_foo"
    assert ds.location == "EU"
    assert ds.create_options == {}
    assert isinstance(ds.resource_strategy, CleanBeforeAndAfter)


def test_change_isolate():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    }).with_test_context("context")
    project = Project("test_project", bq_client=None,
                      bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    ds = ds.isolate()
    assert ds.project.name == "test_project"
    assert ds.name == "dataset_foo"
    assert isinstance(ds.resource_strategy, CleanAfter)
    assert ds.location == "EU"
    assert ds.create_options == {}
    assert ds.isolate_func(ds) == f"{ds.name}_context"
    assert ds.fqdn() == f"{ds.project.name}.{ds.name}_context"


def test_change_create_options():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    }).with_test_context("context")
    project = Project("test_project", bq_client=None,
                      bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    create_options = {"exists_ok": True, "timeout": 10}
    ds = ds.with_create_options(**create_options)
    assert ds.project.name == "test_project"
    assert ds.name == "dataset_foo"
    assert isinstance(ds.resource_strategy, CleanAfter)
    assert ds.isolate_func(ds) == "dataset_foo"
    assert ds.location == "EU"
    assert ds.create_options == create_options
    ds = ds.with_create_options()
    assert ds.create_options == {}


def test_change_location():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    project = Project("test_project", bq_client=None,
                      bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    ds = ds.with_location("US")
    assert ds.project.name == "test_project"
    assert ds.name == "dataset_foo"
    assert isinstance(ds.resource_strategy, CleanAfter)
    assert ds.isolate_func(ds) == "dataset_foo"
    assert ds.create_options == {}
    assert ds.location == "US"
