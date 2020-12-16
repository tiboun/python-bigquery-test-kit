# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

import csv
from copy import deepcopy
from typing import List, Union

from google.cloud.bigquery.schema import SchemaField

from bq_test_kit.data_literal_transformers.base_data_literal_transformer import \
    BaseDataLiteralTransformer
from bq_test_kit.resource_loaders.base_resource_loader import \
    BaseResourceLoader


class DsvDataLiteralTransformer(BaseDataLiteralTransformer):
    """Loader of Delimiter-Seperated Value data. By default, it's CSV.
    """

    def __init__(self):
        """
            Constructor of DsvDataLiteralTransformer.
            Config transformer to load CSV file by default.
        """
        super().__init__()
        self.field_delimiter = ","
        self.quote_character = "\""
        self.escape_character = "\\"
        self.leading_rows_to_skip = 0

    def with_field_delimiter(self, delimiter: str):
        """The field's separator.

        Args:
            delimiter (str): delimiter to use.

        Returns:
            DsvDataLiteralTransformer: new instance of DsvDataLiteralTransformer with updated field_delimiter.
        """
        new_ddlt = deepcopy(self)
        new_ddlt.field_delimiter = delimiter
        return new_ddlt

    def with_quote_character(self, char: str):
        """Character used to quote data sections

        Args:
            char (str): a character.

        Returns:
            DsvDataLiteralTransformer: new instance of DsvDataLiteralTransformer with updated quote character.
        """
        new_ddlt = deepcopy(self)
        new_ddlt.quote_character = char
        return new_ddlt

    def with_escape_character(self, char: str):
        """Character used to quote data sections

        Args:
            char (str): a character.

        Returns:
            DsvDataLiteralTransformer: new instance of DsvDataLiteralTransformer with updated escape character.
        """
        new_ddlt = deepcopy(self)
        new_ddlt.escape_character = char
        return new_ddlt

    def skip_leading_rows(self, nb_lines: int):
        """Number of rows to skip from the beginning of the file.

        Args:
            nb_lines (int): number of lines

        Returns:
            DsvDataLiteralTransformer: new instance of DsvDataLiteralTransformer with updated leading rows to skip.
        """
        new_ddlt = deepcopy(self)
        new_ddlt.leading_rows_to_skip = nb_lines
        return new_ddlt

    def _load(self, datum: Union[BaseResourceLoader, str, List[str]],
              schema_fields: List[SchemaField]) -> str:
        """
            Load a dvs inputs and transform them as data literal, preserving target schema with a fullfilled line.
            This fullfilled line is, of course, discarded from the literal datum.

            Extra columns are put in another column named __extra-columns__.

        Args:
            datum (Union[BaseResourceLoader, str, List[str], None]):
                datum in a file or a string containing lines of datum or a list of data.
            schema List[SchemaField]:
                schema to match with while transforming data to literal.

        Raises:
            DataLiteralTransformException:
                raised when an input data could not be transformed
                as data literal with schema match.

        Returns:
            str: data literal
        """
        csv_lines = self._load_lines_as_array(datum)
        data_csv_lines = csv_lines[self.leading_rows_to_skip:]
        if data_csv_lines:
            rows = csv.DictReader(
                data_csv_lines,
                fieldnames=[f.name for f in schema_fields],
                delimiter=self.field_delimiter,
                quotechar=self.quote_character,
                escapechar=self.escape_character,
                doublequote=False,
                skipinitialspace=False,
                quoting=csv.QUOTE_MINIMAL,
                strict=True,
                restkey="__extra-columns__"
            )
            return self._to_data_literal(rows, schema_fields)
        return self.load([], schema_fields)
