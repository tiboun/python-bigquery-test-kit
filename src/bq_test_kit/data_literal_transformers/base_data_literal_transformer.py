# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

import re
from collections import OrderedDict
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Union

from google.cloud.bigquery.schema import SchemaField

from bq_test_kit.bq_dsl.schema_mixin import SchemaMixin
from bq_test_kit.resource_loaders.base_resource_loader import \
    BaseResourceLoader


class BaseDataLiteralTransformer(SchemaMixin):
    """
        Base of all data literal transformer.
        All data literal transformer must implement load method.
    """
    GEOGRAPHY_RE = re.compile("^POINT\\(\\s*([-+]?\\d+\\.?\\d*)\\s+([-+]?\\d+\\.?\\d*)\\s*\\)\\s*$",
                              flags=re.S | re.IGNORECASE)
    # REGEX that identifies geography point

    TypedDatum = Tuple[Union[BaseResourceLoader, str, List[str]], Union[BaseResourceLoader, str, List[SchemaField]]]

    def __init__(self) -> None:
        """
            Constructor of a data literal transformer.
            Set cast_string_to_bytes as False and cast_datetime_like as False and ignore_unknown_values_flag as False.
            This means that data loading is in strict mode.
        """
        self.cast_string_to_bytes = False
        self.cast_datetime_like = False
        self.ignore_unknown_values_flag = False

    def load(self,
             datum: Union[BaseResourceLoader, str, List[str]],
             schema: Union[BaseResourceLoader, str, List[SchemaField]]) -> str:
        """
            Load inputs and transform them as data literal, preserving target schema with a fullfilled line.
            This fullfilled line is, of course, discarded from the literal datum.

        Args:
            datum (Union[BaseResourceLoader, str, List[str]]):
                datum in a file or a string containing lines of datum or a list of data.
            schema (Union[BaseResourceLoader, str, List[SchemaField]]):
                schema to match with while transforming data to literal.

        Returns:
            str: data literal
        """
        raise NotImplementedError("Must implement load method")

    def load_as(self, datums: Dict[str, TypedDatum]) -> Dict[str, str]:
        """
            Similar to load but load all datum and schema of the given dict
            and return associated data literal for each key.
        """
        raise NotImplementedError("Must implement load_as method")

    def use_string_cast_to_bytes(self, active: bool = True):
        """Use cast expression to transform string to bytes. By default, it convert Base64 string to bytes.

        Args:
            active (bool, optional): enable string cast to bytes. Defaults to True.

        Returns:
            BaseDataLiteralTransformer: A new instance of the current implementation.
        """
        transformer = deepcopy(self)
        transformer.cast_string_to_bytes = active
        return transformer

    def use_datetime_like_cast(self, active: bool = True):
        """Use cast expression to transform datetime like (timestamp, date, datetime, time) to its desired type.
           By default, use adhoc function to convert string properly.

        Args:
            active (bool, optional): enable datetime like cast. Defaults to True.

        Returns:
            BaseDataLiteralTransformer: A new instance of the current implementation.
        """
        transformer = deepcopy(self)
        transformer.cast_datetime_like = active
        return transformer

    def ignore_unknown_values(self, ignore: bool = True):
        """Ignore extra values not represented in the table schema.

        Args:
            ignore (bool, optional): ignore value. Defaults to True.

        Returns:
            BaseDataLoader: new instance of the current data transformer with ignore_unknown_values set to 'ignore'.
        """
        transformer = deepcopy(self)
        transformer.ignore_unknown_values_flag = ignore
        return transformer

    def transform_to_literal(self, json_line: Dict[str, Any], schema: List[SchemaField]):
        """Transform a json line to a data literal.

        Args:
            json_line (Dict[str, Any]): json_line which must be a dictionary since a schema is kind of record.
            schema (List[SchemaField]): schema to match the transformation with.
        """
        # Disabling too many statements since this is related to nested functions.
        # Function scope is only for _transform_to_literal
        # pylint: disable=R0915
        def _transform_repeated_field_to_literal(json_element: Any, schema_field: SchemaField, parent_path: str):
            current_projection = None
            errors = []
            if isinstance(json_element[schema_field.name], list):
                nested_result = None
                if str.upper(schema_field.field_type) == "RECORD":
                    nested_result = [_transform_struct_to_literal(element, schema_field.fields,
                                                                  f"{parent_path}.{schema_field.name}[{i}]", None)
                                     for i, element in enumerate(json_element[schema_field.name])]
                else:
                    nested_result = [_transform_field_to_literal(element, schema_field,
                                                                 f"{parent_path}.{schema_field.name}[{i}]")
                                     for i, element in enumerate(json_element[schema_field.name])]
                nested_errors = [nested_error
                                 for _, nested_errors in nested_result if nested_errors
                                 for nested_error in nested_errors if nested_error]
                if nested_errors:
                    errors.extend(nested_errors)
                else:
                    nested_queries = ", ".join([nested_query for nested_query, _ in nested_result])
                    current_projection = f"[{nested_queries}] as {schema_field.name}"
            else:
                errors.append(f"{parent_path}.{schema_field.name} is not a list while schema is of type "
                              f"{str.upper(schema_field.field_type)} and has mode {schema_field.mode}")
            if errors:
                return None, errors
            return current_projection, None

        def _check_required_field(json_element: Any, schema_field: SchemaField, key: str,
                                  parent_path: str) -> Optional[str]:
            error = None
            if str.upper(schema_field.mode) == "REQUIRED" and (key not in json_element or not json_element[key]):
                error = f"{parent_path}.{key} is required"
            return error

        def _transform_field_to_literal(json_element: Any, schema_field: SchemaField, attribute_path: str):
            # we are handling all the kind of BQ types, that is why we have so many branches.
            # pylint: disable=R0912
            errors = []
            field_projection = None
            if json_element is None:
                field_type = self.generate_data_type(SchemaField(schema_field.name, schema_field.field_type))
                field_projection = f"cast(null as {field_type})"
            elif str.upper(schema_field.field_type) == "GEOGRAPHY":
                if isinstance(json_element, str) and BaseDataLiteralTransformer.GEOGRAPHY_RE.fullmatch(json_element):
                    matches = BaseDataLiteralTransformer.GEOGRAPHY_RE.fullmatch(json_element)
                    x_pos = matches.group(1)
                    y_pos = matches.group(2)
                    field_projection = f"ST_GEOGPOINT({x_pos}, {y_pos})"
                else:
                    errors.append(f"{attribute_path} is a GEOGRAPHY type. "
                                  "It is expected to match POINT(x y) where x and y are FLOAT64. "
                                  f"Instead get {json_element}. "
                                  "POINT is case insensitive.")
            elif isinstance(json_element, str):
                if str.upper(schema_field.field_type) == "STRING":
                    value = json_element.replace("\\", "\\\\")
                    value = value.replace("'", "\\'")
                    field_projection = f"'{value}'"
                elif str.upper(schema_field.field_type) == "BYTES" and not self.cast_string_to_bytes:
                    field_projection = f"from_base64('{json_element}')"
                elif str.upper(schema_field.field_type) == "TIMESTAMP" and not self.cast_datetime_like:
                    field_projection = f"timestamp '{json_element}'"
                elif str.upper(schema_field.field_type) == "DATE" and not self.cast_datetime_like:
                    field_projection = f"date '{json_element}'"
                elif str.upper(schema_field.field_type) == "TIME" and not self.cast_datetime_like:
                    field_projection = f"time '{json_element}'"
                elif str.upper(schema_field.field_type) == "DATETIME" and not self.cast_datetime_like:
                    field_projection = f"datetime '{json_element}'"
                else:
                    value = json_element.replace("\\", "\\\\")
                    value = value.replace("'", "\\'")
                    field_projection = f"cast('{value}' as {str.upper(schema_field.field_type)})"
            elif str.upper(schema_field.field_type) in ["BOOLEAN", "BOOL"]:
                field_projection = (f"{str.lower(str(json_element))}")
            elif str.upper(schema_field.field_type) in ["INTEGER", "INT64"]:
                field_projection = (f"cast({json_element} as INT64)")
            elif str.upper(schema_field.field_type) in ["FLOAT", "FLOAT64"]:
                field_projection = (f"cast({json_element} as FLOAT64)")
            else:
                field_projection = (f"cast({json_element} as {str.upper(schema_field.field_type)})")
            if errors:
                return None, errors
            field_projection = (field_projection if schema_field.mode == "REPEATED"
                                else f"{field_projection} as {schema_field.name}")
            return field_projection, None

        def _transform_struct_to_literal(json_element: Dict[str, Any], schema: List[SchemaField], parent_path: str,
                                         parent_key: Optional[str], parent_schema: SchemaField = None):
            # Disable this too many local variable. Didn't find a way to simplify this.
            # pylint: disable=R0914
            # Disable too many branches since rework may make it less readable (dispatch of functions)
            # pylint: disable=R0912
            current_projection = []
            errors = []
            result = (None, None)
            if isinstance(json_element, dict):
                schema_fields_name = [field.name for field in schema]
                all_keys = list(OrderedDict.fromkeys(schema_fields_name + list(json_element.keys())))
                for key in all_keys:
                    if key not in schema_fields_name:
                        if not self.ignore_unknown_values_flag:
                            errors.append(f"Key {key} not in schema")
                    else:
                        schema_field = next(field for field in schema if field.name == key)
                        error = _check_required_field(json_element, schema_field, key, parent_path)
                        if error:
                            errors.append(error)
                        elif key not in json_element or json_element[key] is None:
                            field_type = self.generate_data_type(schema_field)
                            current_projection.append(f"cast(null as {field_type}) as {key}")
                        else:
                            field_projection, field_errors = None, None
                            if str.upper(schema_field.mode) == "REPEATED":
                                field_projection, field_errors = _transform_repeated_field_to_literal(json_element,
                                                                                                      schema_field,
                                                                                                      parent_path)
                            elif str.upper(schema_field.field_type) == "RECORD":
                                field_projection, field_errors = _transform_struct_to_literal(json_element[key],
                                                                                              schema_field.fields,
                                                                                              f"{parent_path}.{key}",
                                                                                              key,
                                                                                              schema_field.field_type)
                            else:
                                field_projection, field_errors = _transform_field_to_literal(json_element[key],
                                                                                             schema_field,
                                                                                             f"{parent_path}.{key}")
                            if field_errors:
                                errors.extend(field_errors)
                            else:
                                current_projection.append(field_projection)
            elif json_element:
                parent_schema_type = parent_schema.field_type if parent_schema else "RECORD"
                errors.append(f"{parent_path} is not a dictionary while schema is of type {parent_schema_type}")
            if errors:
                result = (None, errors)
            elif current_projection:
                nested_query = ", ".join(current_projection)
                alias = f" as {parent_key}" if parent_key else ""
                final_projection = f"struct({nested_query}){alias}" if parent_path else nested_query
                result = (final_projection, None)
            else:
                # if there is no projection, this means that we have a null struct.
                field_type = self.generate_data_type(schema)
                result = (f"cast(null as {field_type})", None)
            return result
        query, errors = _transform_struct_to_literal(json_line, schema, "", None)
        if query:
            query = f"select {query}"
        return query, errors
