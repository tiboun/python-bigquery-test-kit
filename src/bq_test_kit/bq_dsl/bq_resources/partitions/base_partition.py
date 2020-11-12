# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from google.cloud.bigquery.table import Table as BQTable


class BasePartition:
    """Base of all partitions strategy
    """
    def apply(self, bq_resource: BQTable) -> BQTable:
        """Apply partition strategy to a BQTable in a mutable way
           since BQTable is instanciated at execution time and not at definition time.

        Args:
            bq_resource (BQTable): BQTable to mutate

        Raises:
            NotImplementedError: All partitions strategy must implement this method.

        Returns:
            BQTable: Resource mutated.
        """
        raise NotImplementedError("Method apply not defined.")
