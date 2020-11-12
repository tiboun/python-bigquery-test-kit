# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from bq_test_kit.bq_dsl import Dataset, Project
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.constants import DEFAULT_LOCATION


def test_default_constructor():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    with Project("test_project", bq_client=None, bqtk_config=conf) as p:
        assert p.name == "test_project"


def test_dataset_dsl():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    p = Project("test_project", bq_client=None, bqtk_config=conf)
    d = p.dataset("dataset_foo")
    assert isinstance(d, Dataset)
    assert d.project.dataset("dataset_foo") == d
    assert d.project.name == "test_project"
    d = d.project.dataset("dataset_bar")
    assert len(d.project.datasets) == 2
    assert d.project.datasets[0].project == d.project.datasets[1].project
    assert d.project.datasets[0].name == "dataset_foo"
    assert d.project.datasets[1].name == "dataset_bar"
    assert d.project.name == "test_project"
    d = d.dataset("dataset_foobar")
    assert len(d.project.datasets) == 3
    assert d.project.datasets[0].project == d.project.datasets[1].project == d.project.datasets[2].project
    assert d.project.datasets[0].name == "dataset_foo"
    assert d.project.datasets[1].name == "dataset_bar"
    assert d.project.datasets[2].name == "dataset_foobar"
    assert d.project.name == "test_project"
