# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import os

import pytest

from bq_test_kit.bq_dsl.bq_query_template import BQQueryTemplate
from bq_test_kit.bq_dsl.bq_resources.project import Project
from bq_test_kit.bq_test_kit import BQTestKit
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.exceptions import (ProjectNotDefinedException,
                                    RequirementsException)


def test_project_dsl():
    bqtk = BQTestKit(bq_client=None, bqtk_config=BQTestKitConfig().with_project(name="p1", project_id="p1id"))
    p = bqtk.project("p1")
    assert isinstance(p, Project)


def test_project_dsl_error():
    bqtk = BQTestKit(bq_client=None, bqtk_config=BQTestKitConfig())
    with pytest.raises(ProjectNotDefinedException):
        bqtk.project("undefined")


def test_default_project():
    bqtk = BQTestKit(bq_client=None, bqtk_config=BQTestKitConfig())
    GOOGLE_CLOUD_PROJECT = "GOOGLE_CLOUD_PROJECT"
    project_id = os.environ.get(GOOGLE_CLOUD_PROJECT)
    if project_id:
        assert bqtk.project().name == project_id
    else:
        with pytest.raises(RequirementsException):
            bqtk.project()


def test_get_project_id():
    bqtk = BQTestKit(bq_client=None, bqtk_config=BQTestKitConfig())
    with pytest.raises(RequirementsException):
        bqtk._get_project_id(None, env_var_name="UNDEFINED_VAR")
    os.environ["DEFINED_VAR"] = "DUMMY_PROJECT_ID"
    project = bqtk._get_project_id(None, env_var_name="DEFINED_VAR")
    assert project == "DUMMY_PROJECT_ID"


def test_query_template():
    bqtk = BQTestKit(bq_client=None, bqtk_config=BQTestKitConfig().with_project(name="p1", project_id="p1id"))
    bq_tpl = bqtk.query_template(from_="select 1 as nb")
    assert isinstance(bq_tpl, BQQueryTemplate)
