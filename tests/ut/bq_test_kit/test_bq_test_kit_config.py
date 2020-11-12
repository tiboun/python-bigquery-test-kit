# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest

from bq_test_kit import BQTestKitConfig
from bq_test_kit.constants import DEFAULT_LOCATION, PROJECTS, TEST_CONTEXT
from bq_test_kit.exceptions import InvalidInstanceException


def test_default_constructor():
    bq_test_kit = BQTestKitConfig()
    assert bq_test_kit.config == {}


def test_full_constructor():
    bq_test_kit = BQTestKitConfig({"a": "b"})
    assert "a" in bq_test_kit.config


def test_project():
    bq_test_kit = BQTestKitConfig()
    new_conf = bq_test_kit.with_project(name="n1", project_id="i1")
    assert bq_test_kit.config == {}
    assert new_conf.config == {PROJECTS: {"n1": "i1"}}
    new_conf = new_conf.with_project(name="n2", project_id="i2")
    assert new_conf.config == {
                                PROJECTS: {
                                    "n1": "i1",
                                    "n2": "i2"
                                  }
                              }
    new_conf = new_conf.with_project(name="n2", project_id="i3")
    assert new_conf.config == {
                                PROJECTS: {
                                    "n1": "i1",
                                    "n2": "i3"
                                  }
                              }
    assert new_conf.get_project("n1") == "i1"
    assert new_conf.get_project("n2") == "i3"


def test_with_context():
    bq_test_kit = BQTestKitConfig()
    new_conf = bq_test_kit.with_test_context("_force_1")
    assert bq_test_kit.config == {}
    assert new_conf.config == {TEST_CONTEXT: "_force_1"}
    new_conf = bq_test_kit.with_test_context("_force_2")
    assert new_conf.config == {TEST_CONTEXT: "_force_2"}
    assert new_conf.get_test_context() == "_force_2"


def test_with_invalid_context():
    bq_test_kit = BQTestKitConfig()
    with pytest.raises(InvalidInstanceException):
        bq_test_kit.with_test_context({"obj": "what"})


def test_with_default_location():
    bq_test_kit = BQTestKitConfig()
    new_conf = bq_test_kit.with_default_location("US")
    assert bq_test_kit.config == {}
    assert new_conf.config == {DEFAULT_LOCATION: "US"}
    new_conf = bq_test_kit.with_default_location("EU")
    assert new_conf.config == {DEFAULT_LOCATION: "EU"}
    assert new_conf.get_default_location() == "EU"
