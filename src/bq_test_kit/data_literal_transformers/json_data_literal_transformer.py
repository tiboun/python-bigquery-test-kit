# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

import json
from copy import deepcopy
from json.decoder import JSONDecodeError
from typing import Any, List, Optional, Union

from google.cloud.bigquery.schema import SchemaField
from logzero import logger

from bq_test_kit.data_literal_transformers.base_data_literal_transformer import \
    BaseDataLiteralTransformer
from bq_test_kit.data_literal_transformers.json_format import JsonFormat
from bq_test_kit.exceptions import (InvalidInstanceException,
                                    RowParsingException)
from bq_test_kit.resource_loaders.base_resource_loader import \
    BaseResourceLoader


class JsonDataLiteralTransformer(BaseDataLiteralTransformer):
    """
        Ability to transform json input data as literal, following a given schema.
        JsonDataLiteralTransformer aims to mimic json bq load.
        Using this way avoids loading data into tables.
    """

    def __init__(self, json_format: JsonFormat = JsonFormat.NEWLINE_DELIMITED_JSON) -> None:
        super().__init__()
        self.json_format = json_format

    def with_json_format(self, json_format: JsonFormat):
        """Change json_format

        Args:
            json_format (JsonFormat): Choose one of the enum value.

        Returns:
            JsonDataLiteralTransformer: new instance with json_format changed.
        """
        new_jdlt = deepcopy(self)
        new_jdlt.json_format = json_format
        return new_jdlt

    def _load(self, datum: Union[BaseResourceLoader, str, List[str]],
              schema_fields: List[SchemaField]) -> str:
        """
            Load a json inputs and transform them as data literal, preserving target schema with a fullfilled line.
            This fullfilled line is, of course, discarded from the literal datum.

        Args:
            datum (Union[BaseResourceLoader, str, List[str]]):
                datum in a file or a string containing lines of datum or a list of data.
                If a list of data is given, json_format is ignored, forcing it as a NEWLINE_DELIMITED_JSON.
                NEWLINE_DELIMITED_JSON give a better localisation of where loading fails.
            schema List[SchemaField]:
                schema to match with while transforming data to literal.

        Raises:
            RowParsingException: raised when a line could not be parsed.
            DataLiteralTransformException:
                raised when an input data could not be transformed
                as data literal with schema match.

        Returns:
            str: data literal
        """
        json_lines = None
        if self.json_format == JsonFormat.JSON_ARRAY and not isinstance(datum, list):
            json_lines = self._load_json_array(datum)
        else:
            json_lines = self._load_json_lines(datum)
        return self._to_data_literal(json_lines, schema_fields)

    @staticmethod
    def _load_json_array(datum: Union[BaseResourceLoader, str]) -> List[Any]:
        result = None
        if isinstance(datum, BaseResourceLoader):
            result = datum.load()
        elif isinstance(datum, str):
            result = datum
        else:
            raise InvalidInstanceException(type(datum),
                                           expected_list_instances=[str],
                                           expected_instances=[BaseResourceLoader, str])
        json_array = json.loads(result)
        assert isinstance(json_array, list), 'Given json must be an array'
        return json_array

    def _load_json_lines(self, datum: Union[BaseResourceLoader, str, List[str]]) -> List[Any]:
        datum_lines = self._load_lines_as_array(datum)
        json_lines = [self._load_json_line(i, line) for i, line in enumerate(datum_lines)]
        if any([json_line is None for json_line in json_lines]):
            raise RowParsingException()
        return json_lines

    @staticmethod
    def _load_json_line(line_number: int, line: str) -> Optional[Any]:
        try:
            logger.debug("Converting to json : %s", line)
            return json.loads(line)
        except JSONDecodeError:
            logger.error("An error occured while loading json for line %s", line_number)
            return None
