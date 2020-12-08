# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

import json
from typing import List, Union

from google.cloud.bigquery.schema import SchemaField
from logzero import logger

from bq_test_kit.exceptions import (InvalidInstanceException,
                                    UnexpectedTypeException)
from bq_test_kit.resource_loaders.base_resource_loader import \
    BaseResourceLoader


class SchemaMixin():
    """
        Contains common method related to schema.
    """

    @staticmethod
    def to_schema_field_list(from_: Union[BaseResourceLoader, str, List[SchemaField]]) -> List[SchemaField]:
        """Transform a schema stored in a file, string or a list of SchemaField into a list of SchemaField

        Args:
            from_ (Union[BaseResourceLoader, str, List[SchemaField]]): BigQuery schema

        Raises:
            InvalidInstanceException: not a BaseResourceLoader, str or List[SchemaField]

        Returns:
            List[SchemaField]: Schema fields used by the BigQuery API
        """
        schema_fields = None
        if not isinstance(from_, (BaseResourceLoader, str, list)):
            raise InvalidInstanceException(
                    from_,
                    expected_list_instances=[SchemaField],
                    expected_instances=[BaseResourceLoader, str])
        if isinstance(from_, list) and any([not isinstance(el, SchemaField) for el in from_]):
            raise InvalidInstanceException(
                    from_,
                    expected_list_instances=[SchemaField],
                    expected_instances=[BaseResourceLoader, str])
        if isinstance(from_, (BaseResourceLoader, str)):
            try:
                json_schema_str = from_ if isinstance(from_, str) else from_.load()
                json_schema = json.loads(json_schema_str)
                schema_fields = [SchemaField.from_api_repr(field) for field in json_schema]
            except Exception:
                logger.error("Failed to load schema with %s.", from_)
                raise
            else:
                logger.info("Schema loaded successfully with %s.", from_)
        else:
            schema_fields = from_
        return schema_fields

    @staticmethod
    def generate_data_type(schema: Union[List[SchemaField], SchemaField]):
        """
            Generate fields data type matching the given schema.
            Usefull when data is null and we would like to keep the structure.

        Args:
            schema (Union[List[SchemaField], SchemaField]): schema of a record or a field.
        """
        def _generate_repeated_field_literal_type(schema_field: SchemaField):
            nested_field_type = None
            if str.upper(schema_field.field_type) == "RECORD":
                nested_field_type = _generate_struct_literal_type(schema_field.fields)
            else:
                nested_field_type = _generate_field_literal_type(schema_field)
            field_type = f"ARRAY<{nested_field_type}>"
            return field_type

        def _generate_field_literal_type(schema_field: SchemaField):
            field_type = None
            if str.upper(schema_field.field_type) in ["GEOGRAPHY", "STRING", "BYTES", "NUMERIC", "BOOLEAN", "BOOL",
                                                      "TIMESTAMP", "DATE", "DATETIME", "TIME"]:
                field_type = str.upper(schema_field.field_type)
            elif str.upper(schema_field.field_type) in ["INTEGER", "INT64"]:
                field_type = "INT64"
            elif str.upper(schema_field.field_type) in ["FLOAT", "FLOAT64"]:
                field_type = "FLOAT64"
            else:
                raise UnexpectedTypeException(schema_field.field_type)
            return field_type

        def _generate_struct_literal_type(schema: List[SchemaField]):
            struct_fields_type_list = []
            for schema_field in schema:
                field_type = None
                if str.upper(schema_field.mode) == "REPEATED":
                    field_type = _generate_repeated_field_literal_type(schema_field)
                elif str.upper(schema_field.field_type) == "RECORD":
                    field_type = _generate_struct_literal_type(schema_field.fields)
                else:
                    field_type = _generate_field_literal_type(schema_field)
                struct_fields_type_list.append(f"{schema_field.name} {field_type}")
            struct_fields_type = ", ".join(struct_fields_type_list)
            return f"STRUCT<{struct_fields_type}>"

        field_type = None
        if isinstance(schema, SchemaField):
            if str.upper(schema.mode) == "REPEATED":
                field_type = _generate_repeated_field_literal_type(schema)
            elif str.upper(schema.field_type) == "RECORD":
                field_type = _generate_struct_literal_type(schema.fields)
            else:
                field_type = _generate_field_literal_type(schema)
        else:
            field_type = _generate_struct_literal_type(schema)
        return field_type
