# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from copy import deepcopy
from typing import Any, Dict, Optional

from bq_test_kit.constants import DEFAULT_LOCATION, PROJECTS, TEST_CONTEXT
from bq_test_kit.exceptions import InvalidInstanceException


class BQTestKitConfig():
    """
        BQTestKitConfig act as an immutable builder in order to build config used by bq-test-kit.
    """

    def __init__(self, config: Dict[str, Any] = None) -> None:
        """Constructor of BQTestKitConfig

        Args:
            config (Dict[str, Any], optional): config given at construction time.
                Defaults to {}.
        """
        self.config = config if config else {}

    def with_project(self, *, name: str, project_id: str):
        """Add project. Associate a project name with its id.
           This give the ability to be multi-project such as running those tests
           in an IT GCP project or a DEV GCP project.

        Args:
            name (str): name of the project used in tests
            id (str): GCP project id to connect to.

        Returns:
            BQTestKitConfig: return a new copy of itself before changing data.
        """
        new_conf = deepcopy(self.config)
        projects = new_conf.get(PROJECTS, {})
        projects[name] = project_id
        new_conf[PROJECTS] = projects
        return BQTestKitConfig(new_conf)

    def with_test_context(self, context: str):
        """Give a context used to isolate dataset or tables.

        Args:
            context (str): string to concatenate with

        Raises:
            InvalidInstanceException: Must be a string, otherwise exception is raised.

        Returns:
            BQTestKitConfig: return a new copy of itself before changing data.
        """
        if not isinstance(context, str):
            raise InvalidInstanceException(type(context),
                                           expected_instances=[str])
        new_conf = deepcopy(self.config)
        new_conf[TEST_CONTEXT] = context
        return BQTestKitConfig(new_conf)

    def with_default_location(self, location: str):
        """Location to use in BigQuery api when none is given.

        Args:
            location (str): if no location is specified, use this one.

        Returns:
            BQTestKitConfig: return a new copy of itself before changing data.
        """
        new_conf = deepcopy(self.config)
        new_conf[DEFAULT_LOCATION] = location
        return BQTestKitConfig(new_conf)

    def get_test_context(self) -> Optional[str]:
        """

        Returns:
            Optional[str]: test_context defined or None.
        """
        return self.config.get(TEST_CONTEXT)

    def get_project(self, name: str) -> Optional[str]:
        """

        Args:
            name (str): name of the project to retrieve

        Returns:
            Optional[str]: project id if found or None
        """
        return self.config.get(PROJECTS, {}).get(name)

    def get_default_location(self) -> Optional[str]:
        """

        Returns:
            Optional[str]: default location or None
        """
        return self.config.get(DEFAULT_LOCATION)
