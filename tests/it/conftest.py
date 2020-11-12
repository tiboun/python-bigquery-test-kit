# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# pylint: disable=W0621

"""
    This module provides pytest fixtures used by all integration tests.
"""

import os
import re

import pytest
from google.cloud.bigquery.client import Client
from logzero import logger

from bq_test_kit.bq_test_kit import BQTestKit
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.constants import PROJECTS
from bq_test_kit.exceptions import RequirementsException


@pytest.fixture()
def bqtk_default_context(request) -> str:
    """Generate unique id by using pytest nodeid.

    Args:
        request: pytest fixture which provide tests context info.

    Returns:
        str: unique id matching big query name constraints
    """
    invalid_bq_chars = re.compile('([^a-zA-Z0-9_]+)')
    nodeid = request.node.nodeid
    return invalid_bq_chars.sub('_', nodeid)


@pytest.fixture()
def bqtk(bqtk_config: BQTestKitConfig,
         bqtk_client: Client,
         bqtk_default_context: str) -> BQTestKit:
    """Fixture that build BQTestKit

    Args:
        bqtk_config (BQTestKitConfig): config provided as a fixture
        bqtk_client (Client): BigQuery client provided as a fixture
        bqtk_default_context (str): default context to use if none is defined in config.

    Returns:
        BQTestKit: root of the dsl
    """
    conf = bqtk_config
    if not bqtk_config.get_test_context():
        logger.info("Test context not defined in bqtk_config. Falling back to bqtk_default_context fixture.")
        conf = bqtk_config.with_test_context(bqtk_default_context)
    return BQTestKit(bq_client=bqtk_client, bqtk_config=conf)


@pytest.fixture(autouse=True)
def bqtk_requirements(bqtk_config: BQTestKitConfig) -> None:
    """Check requirements of it testing, therefore capability to interact with BigQuery.

    Args:
        bqtk_config (BQTestKitConfig): config provided as a fixture

    Raises:
        RequirementsException: list of errors raised as one exception.

    """
    errors = []
    if PROJECTS not in bqtk_config.config:
        errors.append(f"Key '{PROJECTS}' not found in bqtk_config.config.")
    if errors:
        raise RequirementsException(",\n\t".join(errors))


@pytest.fixture()
def bqtk_config() -> BQTestKitConfig:
    """Provide bq-test-kit config as a fixture used by all it tests.

    Raises:
        Exception: Ensure that GOOGLE_CLOUD_PROJECT env var is available for it tests.

    Returns:
        BQTestKitConfig: config with a project configured with 'it' name.
    """
    GOOGLE_CLOUD_PROJECT = "GOOGLE_CLOUD_PROJECT"
    project = os.environ.get(GOOGLE_CLOUD_PROJECT)
    if not project:
        raise Exception(f"{GOOGLE_CLOUD_PROJECT} env var is not defined for it test."
                        " Please set the default project.")
    return BQTestKitConfig().with_project(
        name="it",
        project_id=project)


@pytest.fixture()
def bqtk_client() -> Client:
    """BigQuery client as a fixture

    Returns:
        Client: BigQuery client
    """
    return Client(location="EU")
