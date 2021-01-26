# Copyright (c) 2021 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

# Disabled check of cyclic import
# pylint: disable=R0401

from copy import deepcopy
from typing import TYPE_CHECKING

from bq_test_kit.data_literal_transformers.base_data_literal_transformer import \
    BaseDataLiteralTransformer
from bq_test_kit.typing import TableResources

if TYPE_CHECKING:
    from bq_test_kit.bq_dsl.bq_query_template import BQQueryTemplate


class BQQueryDatum():
    """Dsl used for datum injected to the query.
    """
    def __init__(self, bq_query_template: 'BQQueryTemplate', as_temp_tables: bool,
                 tables: TableResources):
        """Constructor of BQQueryDatum

        Args:
            bq_query_template (BQQueryTemplate): bq_query_template to update
            as_temp_tables (bool): inject datum as temp tables or data literals
            tables (TableResources): list of all tables
        """
        self.bq_query_template = bq_query_template
        self.tables = tables
        self.use_temp_tables = as_temp_tables

    def as_data_literals(self) -> 'BQQueryDatum':
        """inject given datum as data literals

        Returns:
            BQQueryDatum: new instance with use_temp_tables as False
        """
        bq_datum = deepcopy(self)
        bq_datum.use_temp_tables = False
        return bq_datum

    def as_temp_tables(self) -> 'BQQueryDatum':
        """inject given datum as temp tables

        Returns:
            BQQueryDatum: new instance with use_temp_tables as True
        """
        bq_datum = deepcopy(self)
        bq_datum.use_temp_tables = True
        return bq_datum

    def loaded_with(self, transformer: BaseDataLiteralTransformer) -> 'BQQueryTemplate':
        """Register datum to be injected once query is run.

        Returns:
            BQQueryTemplate: new instance with use_temp_tables as True
        """
        if self.use_temp_tables:
            return self.bq_query_template.with_temp_tables((transformer, self.tables))
        queries_as_dict = transformer.load_as(self.tables)
        return self.bq_query_template.with_global_dict(queries_as_dict)
