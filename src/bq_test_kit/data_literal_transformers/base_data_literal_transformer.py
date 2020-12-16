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
from logzero import logger

from bq_test_kit.bq_dsl.schema_mixin import SchemaMixin
from bq_test_kit.exceptions import (DataLiteralTransformException,
                                    InvalidInstanceException)
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
             datum: Union[BaseResourceLoader, str, List[str], None],
             schema: Union[BaseResourceLoader, str, List[SchemaField]]) -> str:
        """
            Load inputs and transform them as data literal, preserving target schema with a fullfilled line.
            This fullfilled line is, of course, discarded from the literal datum.

            If datum is empty as the python way, then an empty line is generated.
            This means that BaseResourceLoader is not considered as empty.

        Args:
            datum (Union[BaseResourceLoader, str, List[str], None]):
                datum in a file or a string containing lines of datum or a list of data or None.
            schema (Union[BaseResourceLoader, str, List[SchemaField]]):
                schema to match with while transforming data to literal.

        Returns:
            str: data literal
        """
        schema_fields = self.to_schema_field_list(schema)
        return self._load(datum, schema_fields) if datum else self._empty_literal(schema_fields)

    def _empty_literal(self, schema_fields: List[SchemaField]):
        query, transform_errors = self.transform_to_literal({}, schema_fields)
        if transform_errors:
            errors_str = ",\n".join(["\t" + error for error in transform_errors])
            raise DataLiteralTransformException(f"Exception happened with the following errors :\n{errors_str}")
        query = f"(select * from ({query}) limit 0)"
        logger.info("Empty literal generated.")
        logger.debug("Empty literal generated as :\n%s", query)
        return query

    def _load(self,
              datum: Union[BaseResourceLoader, str, List[str]],
              schema_fields: List[SchemaField]) -> str:
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

    def _to_data_literal(self, rows: List[Dict[str, Any]], schema_fields: List[SchemaField]):
        queries = []
        errors = []
        for i, row in enumerate(rows):
            query, transform_errors = self.transform_to_literal(row, schema_fields)
            if transform_errors:
                errors_str = ",\n".join(["\t" + error for error in transform_errors])
                errors.append(f"Exception happened in line {i+1} with the following errors :\n{errors_str}")
            else:
                queries.append(query)
        if errors:
            raise DataLiteralTransformException("\n\n".join(errors))
        query_result = "(" + "\nunion all\n".join(queries) + ")"
        logger.info("Datum has been transformed.")
        logger.debug("Datum has been transformed to \n%s", query_result)
        return query_result

    def load_as(self, datums: Dict[str, TypedDatum]) -> Dict[str, str]:
        """
            Similar to load but load all datum and schema of the given dict
            and return associated data literal for each key.
        """
        errors = []

        def _load(key, datum, schema):
            try:
                return self.load(datum, schema)
            # Catch all kind of exception in order to append all of them and raise all errors at once.
            # pylint: disable=W0703
            except Exception as error:
                errors.append(f"key {key} failed at loading : {error}")
        data_literals = {key: _load(key, datum, schema) for key, (datum, schema) in datums.items()}
        if errors:
            raise DataLiteralTransformException("\n".join(errors))
        return data_literals

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

    def transform_to_literal(self, data_line: Dict[str, Any], schema: List[SchemaField]):
        """Transform dictionary to a data literal matching the given schema.

        Args:
            data_line (Dict[str, Any]): data_line which must be a dictionary since a schema is kind of record.
            schema (List[SchemaField]): schema to match the transformation with.
        """
        # Disabling too many statements since this is related to nested functions.
        # Function scope is only for _transform_to_literal
        # pylint: disable=R0915
        def _transform_repeated_field_to_literal(data_element: Any, schema_field: SchemaField, parent_path: str):
            current_projection = None
            errors = []
            if isinstance(data_element[schema_field.name], list):
                nested_result = None
                if str.upper(schema_field.field_type) == "RECORD":
                    nested_result = [_transform_struct_to_literal(element, schema_field.fields,
                                                                  f"{parent_path}.{schema_field.name}[{i}]", None)
                                     for i, element in enumerate(data_element[schema_field.name])]
                else:
                    nested_result = [_transform_field_to_literal(element, schema_field,
                                                                 f"{parent_path}.{schema_field.name}[{i}]")
                                     for i, element in enumerate(data_element[schema_field.name])]
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

        def _check_required_field(data_element: Any, schema_field: SchemaField, parent_path: str) -> Optional[str]:
            error = None
            key = schema_field.name
            if str.upper(schema_field.mode) == "REQUIRED" and (key not in data_element or not data_element[key]):
                error = f"{parent_path}.{key} is required"
            return error

        def _transform_field_to_literal(data_element: Any, schema_field: SchemaField, attribute_path: str):
            # we are handling all the kind of BQ types, that is why we have so many branches.
            # pylint: disable=R0912
            errors = []
            field_projection = None
            if data_element is None:
                field_type = self.generate_data_type(SchemaField(schema_field.name, schema_field.field_type))
                field_projection = f"cast(null as {field_type})"
            elif str.upper(schema_field.field_type) == "GEOGRAPHY":
                if isinstance(data_element, str) and BaseDataLiteralTransformer.GEOGRAPHY_RE.fullmatch(data_element):
                    matches = BaseDataLiteralTransformer.GEOGRAPHY_RE.fullmatch(data_element)
                    x_pos = matches.group(1)
                    y_pos = matches.group(2)
                    field_projection = f"ST_GEOGPOINT({x_pos}, {y_pos})"
                else:
                    errors.append(f"{attribute_path} is a GEOGRAPHY type. "
                                  "It is expected to match POINT(x y) where x and y are FLOAT64. "
                                  f"Instead get {data_element}. "
                                  "POINT is case insensitive.")
            elif str.upper(schema_field.field_type) == "STRING":
                value = str(data_element).replace("\\", "\\\\")
                value = value.replace("'", "\\'")
                field_projection = f"'{value}'"
            elif str.upper(schema_field.field_type) == "BYTES" and not self.cast_string_to_bytes:
                field_projection = f"from_base64('{data_element}')"
            elif str.upper(schema_field.field_type) == "TIMESTAMP" and not self.cast_datetime_like:
                field_projection = f"timestamp '{data_element}'"
            elif str.upper(schema_field.field_type) == "DATE" and not self.cast_datetime_like:
                field_projection = f"date '{data_element}'"
            elif str.upper(schema_field.field_type) == "TIME" and not self.cast_datetime_like:
                field_projection = f"time '{data_element}'"
            elif str.upper(schema_field.field_type) == "DATETIME" and not self.cast_datetime_like:
                field_projection = f"datetime '{data_element}'"
            elif str.upper(schema_field.field_type) in ["BOOLEAN", "BOOL"]:
                field_projection = (f"{str.lower(str(data_element))}")
            elif str.upper(schema_field.field_type) in ["INTEGER", "INT64"]:
                field_projection = (f"cast({data_element} as INT64)")
            elif str.upper(schema_field.field_type) in ["FLOAT", "FLOAT64"]:
                field_projection = (f"cast({data_element} as FLOAT64)")
            else:
                value = data_element
                if str.upper(schema_field.field_type) in ["TIMESTAMP", "DATE", "TIME", "DATETIME", "BYTES"]:
                    value = str(data_element).replace("\\", "\\\\")
                    value = value.replace("'", "\\'")
                    value = f"'{value}'"
                field_projection = f"cast({value} as {str.upper(schema_field.field_type)})"
            if errors:
                return None, errors
            field_projection = (field_projection if schema_field.mode == "REPEATED"
                                else f"{field_projection} as {schema_field.name}")
            return field_projection, None

        def _transform_struct_to_literal(data_element: Dict[str, Any], schema: List[SchemaField], parent_path: str,
                                         key: Optional[str], parent_schema: SchemaField = None):
            # Disable this too many local variable. Didn't find a way to simplify this.
            # pylint: disable=R0914
            # Disable too many branches since rework may make it less readable (dispatch of functions)
            # pylint: disable=R0912
            current_projection = []
            errors = []
            result = (None, None)
            if isinstance(data_element, dict):
                schema_fields_name = [field.name for field in schema]
                all_keys = list(OrderedDict.fromkeys(schema_fields_name + list(data_element.keys())))
                for child_key in all_keys:
                    if child_key not in schema_fields_name:
                        if not self.ignore_unknown_values_flag:
                            errors.append(f"Key {child_key} not in schema")
                    else:
                        schema_field = next(field for field in schema if field.name == child_key)
                        error = _check_required_field(data_element, schema_field, parent_path)
                        if error:
                            errors.append(error)
                        elif child_key not in data_element or data_element[child_key] is None:
                            field_type = self.generate_data_type(schema_field)
                            current_projection.append(f"cast(null as {field_type}) as {child_key}")
                        else:
                            field_projection, field_errors = None, None
                            if str.upper(schema_field.mode) == "REPEATED":
                                field_projection, field_errors = _transform_repeated_field_to_literal(data_element,
                                                                                                      schema_field,
                                                                                                      parent_path)
                            elif str.upper(schema_field.field_type) == "RECORD":
                                child_path = f"{parent_path}.{child_key}"
                                field_projection, field_errors = _transform_struct_to_literal(data_element[child_key],
                                                                                              schema_field.fields,
                                                                                              child_path,
                                                                                              child_key,
                                                                                              schema_field)
                            else:
                                child_path = f"{parent_path}.{child_key}"
                                field_projection, field_errors = _transform_field_to_literal(data_element[child_key],
                                                                                             schema_field,
                                                                                             child_path)
                            if field_errors:
                                errors.extend(field_errors)
                            else:
                                current_projection.append(field_projection)
            elif data_element:
                parent_schema_type = parent_schema.field_type if parent_schema else "RECORD"
                errors.append(f"{parent_path} is not a dictionary while schema is of type {parent_schema_type}")
            if errors:
                result = (None, errors)
            elif current_projection:
                nested_query = ", ".join(current_projection)
                alias = f" as {key}" if key else ""
                final_projection = f"struct({nested_query}){alias}" if parent_path else nested_query
                result = (final_projection, None)
            else:
                # if there is no projection, this means that we have a null struct.
                field_type = self.generate_data_type(schema)
                alias = f" as {key}" if key else ""
                final_projection = f"cast(null as {field_type}){alias}"
                result = (final_projection, None)
            return result
        query, errors = _transform_struct_to_literal(data_line, schema, "", None)
        if query:
            query = f"select {query}"
        return query, errors

    @staticmethod
    def _load_lines_as_array(datum: Union[BaseResourceLoader, str, List[str]]) -> List[Any]:
        datum_lines = None
        if isinstance(datum, BaseResourceLoader):
            datum_lines = datum.load().splitlines(keepends=False)
        elif isinstance(datum, str):
            datum_lines = datum.splitlines(keepends=False)
        elif isinstance(datum, list) and (len(datum) == 0 or isinstance(datum[0], str)):
            datum_lines = datum
        else:
            raise InvalidInstanceException(type(datum),
                                           expected_list_instances=[str],
                                           expected_instances=[BaseResourceLoader, str])
        return datum_lines
