# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from base64 import b64encode
from typing import Any, Dict, List

from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Row, RowIterator


class BQQueryResult():
    """
        Wrap BigQuery RowIterator in order to add additional features to it.
    """
    def __init__(self, row_iterator: RowIterator) -> None:
        self._row_iterator = row_iterator
        self._rows = list(row_iterator)
        self._rows_dict = self._convert_row_to_dict(self._rows)

    @property
    def schema(self) -> List[SchemaField]:
        """
        Returns:
            List[SchemaField]: Schema of the output
        """
        return self._row_iterator.schema

    @property
    def rows_bq(self) -> List[Row]:
        """
        Returns:
            List[Row]: native BigQuery rows
        """
        return self._rows

    @property
    def rows(self) -> List[Dict[str, Any]]:
        """Transform BigQuery rows as dict.
           Transform array of bytes into base64 encoded string.

        Returns:
            List[Dict[str, Any]]: [description]
        """
        return self._rows_dict

    @property
    def total_rows(self) -> int:
        """
        Returns:
            int: total rows of the query
        """
        return self._row_iterator.total_rows

    @staticmethod
    def _convert_row_to_dict(row_iterator: List[Row]) -> List[Dict[str, Any]]:
        def _convert_type(element: Any):
            convertion_result = element
            if isinstance(element, bytes):
                convertion_result = b64encode(element).decode('ascii')
            elif isinstance(element, (dict, Row)):
                convertion_result = {k: _convert_type(v) for k, v in element.items()}
            elif isinstance(element, list):
                convertion_result = [_convert_type(v) for v in element]
            return convertion_result

        result = [_convert_type(row) for row in row_iterator]
        return result
