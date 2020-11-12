# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest

from bq_test_kit.bq_dsl.bq_resources.project import Project
from bq_test_kit.bq_dsl.bq_resources.tables import Tables
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.constants import DEFAULT_LOCATION
from bq_test_kit.exceptions import InvalidInstanceException


def test_resources_flattening():
    conf = BQTestKitConfig({
        DEFAULT_LOCATION: "EU"
    })
    p = Project("test_project", bq_client=None, bqtk_config=conf) \
        .dataset("dataset_foo") \
        .table("table_foofoo") \
        .table("table_foobar") \
        .project.dataset("dataset_bar") \
        .table("table_barfoo") \
        .table("table_barbar") \
        .project
    resources = Tables.from_(p)._flatten_bq_resources()
    resource_names = [resource.name for resource in resources]
    assert resource_names == ["test_project",
                              "dataset_foo",
                              "dataset_bar",
                              "table_foofoo",
                              "table_foobar",
                              "table_barfoo",
                              "table_barbar"]
    resources = Tables.from_(p, p)._flatten_bq_resources()
    resource_names = [resource.name for resource in resources]
    assert resource_names == ["test_project",
                              "dataset_foo",
                              "dataset_bar",
                              "table_foofoo",
                              "table_foobar",
                              "table_barfoo",
                              "table_barbar"]
    resources = Tables.from_(p.dataset("dataset_bar"), p.dataset("dataset_foo"))._flatten_bq_resources()
    resource_names = [resource.name for resource in resources]
    assert resource_names == ["dataset_bar",
                              "table_barfoo",
                              "table_barbar",
                              "dataset_foo",
                              "table_foofoo",
                              "table_foobar"
                              ]
    resources = Tables.from_(p.dataset("dataset_bar"),
                             p.dataset("dataset_foo"),
                             p.dataset("dataset_foo"),
                             p.dataset("dataset_bar"))._flatten_bq_resources()
    resource_names = [resource.name for resource in resources]
    assert resource_names == ["dataset_bar",
                              "table_barfoo",
                              "table_barbar",
                              "dataset_foo",
                              "table_foofoo",
                              "table_foobar"
                              ]
    resources = Tables.from_(p.dataset("dataset_bar").table("table_barfoo"),
                             p.dataset("dataset_foo").table("table_foofoo"),
                             p.dataset("dataset_foo").table("table_foofoo"),
                             p.dataset("dataset_bar").table("table_barbar"))._flatten_bq_resources()
    resource_names = [resource.name for resource in resources]
    assert resource_names == ["table_barfoo",
                              "table_foofoo",
                              "table_barbar"
                              ]


def test_invalid_resource():
    with pytest.raises(InvalidInstanceException):
        Tables.from_(1)._flatten_bq_resources()
