# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

import os
from typing import List, Optional, Union

from google.cloud.bigquery.client import Client
from logzero import logger

from bq_test_kit.bq_dsl import BQQueryTemplate
from bq_test_kit.bq_dsl.bq_resources.project import Project
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.constants import GOOGLE_CLOUD_PROJECT
from bq_test_kit.exceptions import (ProjectNotDefinedException,
                                    RequirementsException)
from bq_test_kit.interpolators.base_interpolator import BaseInterpolator
from bq_test_kit.resource_loaders import BaseResourceLoader


class BQTestKit():
    """
        bq-test-kit is the root of the DSL. It enables you to define datasets and tables inside projects
        as well as running query templates.
    """
    def __init__(self,
                 *, bq_client: Client, bqtk_config: BQTestKitConfig) -> None:
        """Constructor of bq-test-kit

        Args:
            bq_client (Client): instance of bigquery client to use accross the DSL.
            bqtk_config (BQTestKitConfig): config used accross the DSL.
        """
        self._bq_client = bq_client
        self.bqtk_config = bqtk_config

    def project(self, name: Optional[str] = None) -> Project:
        """Retrieve a specific project by name from BQTestKitCOnfig, otherwise fallback to the default one.

        Args:
            name (Optional[str], optional): name of the project to retrieve. Defaults to None.

        Raises:
            ProjectNotDefinedException: thrown when project name is not given inside BQTestKitConfig or
                if default project is not defined.

        Returns:
            Project: project DSL.
        """
        project_id = self._get_project_id(name)
        if project_id:
            return Project(project_id, bq_client=self._bq_client,
                           bqtk_config=self.bqtk_config)
        raise ProjectNotDefinedException(name)

    def query_template(self,
                       *, from_: Union[BaseResourceLoader, str],
                       interpolators: List[BaseInterpolator] = None) -> BQQueryTemplate:
        """Go to the query template dsl in order to run query against BigQuery.

        Args:
            from_ (Union[BaseResourceLoader, str]): query may be loaded with a BaseResourceLoader or be a string
            interpolators (List[BaseInterpolator], optional): List of interpolator used to inerpolate the given query.
                Defaults to None.

        Returns:
            BQQueryTemplate: query DSL
        """
        return BQQueryTemplate(from_=from_, bqtk_config=self.bqtk_config,
                               bq_client=self._bq_client, interpolators=interpolators)

    def _get_project_id(self, name: Optional[str] = None,
                        *, env_var_name: str = GOOGLE_CLOUD_PROJECT) -> Optional[str]:
        project_id = None
        if name is None:
            project_id = os.environ.get(env_var_name)
            if not project_id:
                raise RequirementsException(f"{env_var_name} env var is not defined."
                                            " Please set it as this is used as the default project.")
            logger.info("Falling back to default project %s", project_id)
        else:
            project_id = self.bqtk_config.get_project(name)
        return project_id
