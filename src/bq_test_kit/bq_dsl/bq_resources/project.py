# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from copy import deepcopy
from typing import List

from google.cloud.bigquery.client import Client

from bq_test_kit.bq_dsl.bq_resources.base_bq_resource import BaseBQResource
from bq_test_kit.bq_dsl.bq_resources.dataset import Dataset
from bq_test_kit.bq_test_kit_config import BQTestKitConfig


class Project(BaseBQResource):
    """Project DSL which allows you to define datasets.

    Args:
        BaseBQResource (BaseBQResource): Project is a BigQuery resource.
    """

    def __init__(self, name: str,
                 *, bq_client: Client, bqtk_config: BQTestKitConfig, datasets: List[Dataset] = None) -> None:
        """Constructor of Project

        Args:
            bq_client (Client): instance of bigquery client to use accross the DSL.
            bqtk_config (BQTestKitConfig): config used accross the DSL.
            dataets (List[Dataset], optional): list of all datasets created with the DSL belonging to this project.
                Default to [].
        """
        super().__init__(name=name, bq_client=bq_client,
                         bqtk_config=bqtk_config)
        self.datasets = datasets if datasets else []

    def dataset(self, name: str) -> Dataset:
        """Go down one step to the Dataset DSL. Dataset is linked to the current project.

        Args:
            name (str): name of the dataset

        Returns:
            Dataset: dataset DSL.
        """
        dataset = next((d for d in self.datasets if d.name == name), None)
        if not dataset:
            new_project = deepcopy(self)
            dataset = Dataset(name, project=new_project, bq_client=self._bq_client, bqtk_config=self.bqtk_config)
            new_project.datasets.append(dataset)
        return dataset

    def fqdn(self) -> str:
        return self.name

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> None:
        pass

    def __deepcopy__(self, memo):
        new_project = Project(
            deepcopy(self.name, memo),
            # copy is not done because bq client have non-trivial state
            # that is local and unpickleable
            bq_client=self._bq_client,
            bqtk_config=deepcopy(self.bqtk_config, memo),
            datasets=deepcopy(self.datasets, memo)
        )
        for dataset in new_project.datasets:
            dataset.project = new_project
        return new_project
