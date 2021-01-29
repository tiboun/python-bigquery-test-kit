# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest

from bq_test_kit.data_literal_transformers import JsonDataLiteralTransformer
from bq_test_kit.data_literal_transformers.json_format import JsonFormat
from bq_test_kit.exceptions import (DataLiteralTransformException,
                                    InvalidInstanceException,
                                    RowParsingException)
from bq_test_kit.resource_loaders.package_file_loader import PackageFileLoader


@pytest.mark.parametrize("json_input", [
    ('{"f_string": "str with quote \' and backslash \\\\", "f_bytes": "YW55", "f_int": 1, "f_float": 1.5,'
     '"f_bool": true, "f_timestamp": "2020-11-26 17:09:03.967259 UTC", "f_date": "2020-11-26", "f_time": "11:09:03",'
     '"f_datetime": "2020-11-26T17:09:03", "f_numeric": 1.6, "f_geography": "POINT(-122.35 47.62)"}'),
    ['{"f_string": "str with quote \' and backslash \\\\", "f_bytes": "YW55", "f_int": 1, "f_float": 1.5,'
     '"f_bool": true, "f_timestamp": "2020-11-26 17:09:03.967259 UTC", "f_date": "2020-11-26", "f_time": "11:09:03",'
     '"f_datetime": "2020-11-26T17:09:03", "f_numeric": 1.6, "f_geography": "POINT(-122.35 47.62)"}'],
    PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema_datum.json")
 ])
def test_json_load_simple_schema(json_input):
    transformer = JsonDataLiteralTransformer().use_datetime_like_cast()
    query = transformer.load(
        json_input,
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
    )
    assert query == ("(select 'str with quote \\' and backslash \\\\' as f_string, "
                     "from_base64('YW55') as f_bytes, cast(1 as INT64) as f_int, cast(1.5 as FLOAT64) as f_float, "
                     "true as f_bool, "
                     "cast('2020-11-26 17:09:03.967259 UTC' as TIMESTAMP) as f_timestamp, "
                     "cast('2020-11-26' as DATE) as f_date, cast('11:09:03' as TIME) as f_time, "
                     "cast('2020-11-26T17:09:03' as DATETIME) as f_datetime, cast(1.6 as NUMERIC) as f_numeric, "
                     "ST_GEOGPOINT(-122.35, 47.62) as f_geography, "
                     "cast(null as STRUCT<f_datetime DATETIME>) as f_struct)")


@pytest.mark.parametrize("json_input", [
    """
    [
        {
            "f_string": "str with quote ' and backslash \\\\",
            "f_bytes": "YW55",
            "f_int": 1,
            "f_float": 1.5,
            "f_bool": true,
            "f_timestamp": "2020-11-26 17:09:03.967259 UTC",
            "f_date": "2020-11-26",
            "f_time": "11:09:03",
            "f_datetime": "2020-11-26T17:09:03",
            "f_numeric": 1.6,
            "f_geography": "POINT(-122.35 47.62)"
        }
    ]
    """,
    ["""
        {
            "f_string": "str with quote ' and backslash \\\\",
            "f_bytes": "YW55",
            "f_int": 1,
            "f_float": 1.5,
            "f_bool": true,
            "f_timestamp": "2020-11-26 17:09:03.967259 UTC",
            "f_date": "2020-11-26",
            "f_time": "11:09:03",
            "f_datetime": "2020-11-26T17:09:03",
            "f_numeric": 1.6,
            "f_geography": "POINT(-122.35 47.62)"
        }
    """],
    PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema_datum_array.json")
 ])
def test_json_array_load_simple_schema(json_input):
    transformer = JsonDataLiteralTransformer(json_format=JsonFormat.JSON_ARRAY)
    query = transformer.load(
        json_input,
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json"),
    )
    assert query == ("(select 'str with quote \\' and backslash \\\\' as f_string, "
                     "from_base64('YW55') as f_bytes, cast(1 as INT64) as f_int, cast(1.5 as FLOAT64) as f_float, "
                     "true as f_bool, "
                     "timestamp '2020-11-26 17:09:03.967259 UTC' as f_timestamp, "
                     "date '2020-11-26' as f_date, time '11:09:03' as f_time, "
                     "datetime '2020-11-26T17:09:03' as f_datetime, cast(1.6 as NUMERIC) as f_numeric, "
                     "ST_GEOGPOINT(-122.35, 47.62) as f_geography, "
                     "cast(null as STRUCT<f_datetime DATETIME>) as f_struct)")


def test_json_load_simple_schema_with_string_cast_to_bytes():
    transformer = JsonDataLiteralTransformer().use_string_cast_to_bytes().use_datetime_like_cast()
    query = transformer.load(
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/"
                          "simple_schema_datum_with_string_cast_to_bytes.json"),
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
    )
    assert query == ("(select 'str with quote \\' and backslash \\\\' as f_string, "
                     "cast('any' as BYTES) as f_bytes, cast(1 as INT64) as f_int, cast(1.5 as FLOAT64) as f_float, "
                     "true as f_bool, "
                     "cast('2020-11-26 17:09:03.967259 UTC' as TIMESTAMP) as f_timestamp, "
                     "cast('2020-11-26' as DATE) as f_date, cast('11:09:03' as TIME) as f_time, "
                     "cast('2020-11-26 17:09:03' as DATETIME) as f_datetime, cast(1.6 as NUMERIC) as f_numeric, "
                     "ST_GEOGPOINT(-122.35, 47.62) as f_geography, "
                     "cast(null as STRUCT<f_datetime DATETIME>) as f_struct)")


def test_invalid_json_load():
    transformer = JsonDataLiteralTransformer()
    with pytest.raises(RowParsingException):
        transformer.load(
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/invalid_json_datum.txt"),
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
        )


def test_json_load_with_extra_column():
    transformer = JsonDataLiteralTransformer()
    with pytest.raises(DataLiteralTransformException) as exception:
        transformer.load(
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/"
                              "simple_schema_with_extra_datum.json"),
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
        )
    assert str(exception.value) == ("Exception happened in line 1 with the following errors :\n"
                                    "\tKey f_string_extra @ . not in schema\n\n"
                                    "Exception happened in line 2 with the following errors :\n"
                                    "\tKey f_string_extra2 @ . not in schema")
    query = transformer.ignore_unknown_values().use_datetime_like_cast().load(
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/"
                          "simple_schema_with_extra_datum.json"),
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
    )
    assert query == ("(select 'str with quote \\' and backslash \\\\' as f_string, "
                     "from_base64('YW55') as f_bytes, cast(1 as INT64) as f_int, cast(1.5 as FLOAT64) as f_float, "
                     "true as f_bool, "
                     "cast('2020-11-26 17:09:03.967259 UTC' as TIMESTAMP) as f_timestamp, "
                     "cast('2020-11-26' as DATE) as f_date, cast('11:09:03' as TIME) as f_time, "
                     "cast('2020-11-26 17:09:03' as DATETIME) as f_datetime, cast(1.6 as NUMERIC) as f_numeric, "
                     "ST_GEOGPOINT(-122.35, 47.62) as f_geography, "
                     "cast(null as STRUCT<f_datetime DATETIME>) as f_struct"
                     "\nunion all\n"
                     "select 'other string' as f_string, "
                     "from_base64('YW55') as f_bytes, cast(2 as INT64) as f_int, cast(2.5 as FLOAT64) as f_float, "
                     "true as f_bool, "
                     "cast('2020-11-26 17:09:03.967259 UTC' as TIMESTAMP) as f_timestamp, "
                     "cast('2020-11-26' as DATE) as f_date, cast('11:09:03' as TIME) as f_time, "
                     "cast('2020-11-26 17:09:03' as DATETIME) as f_datetime, cast(2.6 as NUMERIC) as f_numeric, "
                     "ST_GEOGPOINT(-122.50, -40.6) as f_geography, "
                     "cast(null as STRUCT<f_datetime DATETIME>) as f_struct)")


def test_json_load_nested_schema():
    transformer = JsonDataLiteralTransformer().use_datetime_like_cast()
    query = transformer.load(
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/nested_schema_datum.json"),
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/nested_schema.json")
    )
    assert query == ("(select struct('str with quote \\' and backslash \\\\' as f_string, "
                     "from_base64('YW55') as f_bytes, cast(1 as INT64) as f_int, cast(1.5 as FLOAT64) as f_float, "
                     "true as f_bool, "
                     "cast('2020-11-26 17:09:03.967259 UTC' as TIMESTAMP) as f_timestamp, "
                     "cast('2020-11-26' as DATE) as f_date, cast('11:09:03' as TIME) as f_time, "
                     "cast('2020-11-26 17:09:03' as DATETIME) as f_datetime, cast(1.6 as NUMERIC) as f_numeric, "
                     "cast(null as STRUCT<f_string STRING>) as f_nested2) as f_nested)")


def test_json_load_repeated_schema():
    transformer = JsonDataLiteralTransformer().use_datetime_like_cast()
    query = transformer.load(
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/"
                          "repeated_schema_datum.json"),
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/repeated_schema.json")
    )
    assert query == ("(select ['str with quote \\'', cast(null as STRING), "
                     "'and backslash \\\\'] as f_string, [from_base64('YW55'), "
                     "from_base64('c3RyaW5n'), cast(null as BYTES)] as f_bytes, [cast(null as INT64), "
                     "cast(1 as INT64), cast(2 as INT64)] as f_int, [cast(1.5 as FLOAT64), cast(null as FLOAT64), "
                     "cast(2.6 as FLOAT64)] as f_float, "
                     "[true, false, cast(null as BOOLEAN)] as f_bool, "
                     "[cast(null as TIMESTAMP), cast('2020-11-26 17:09:03.967259 UTC' as TIMESTAMP), "
                     "cast('2019-11-26 17:09:03.967259 UTC' as TIMESTAMP)] as f_timestamp, "
                     "[cast('2020-11-26' as DATE), cast(null as DATE), cast('2019-11-26' as DATE)] as f_date, "
                     "[cast('11:09:03' as TIME), cast('10:09:03' as TIME), cast(null as TIME)] as f_time, "
                     "[cast(null as DATETIME), cast('2020-11-26 17:09:03' as DATETIME), "
                     "cast('2019-11-26 17:09:03' as DATETIME)] as f_datetime, [cast(1.6 as NUMERIC), "
                     "cast(null as NUMERIC), cast(1.7 as NUMERIC)] as f_numeric, "
                     "[struct(cast(null as ARRAY<STRING>) as f_string, [from_base64('dG9fYnl0ZXM=')] as f_bytes), "
                     "struct(['a_string'] as f_string, cast(null as ARRAY<BYTES>) as f_bytes), "
                     "cast(null as STRUCT<f_string ARRAY<STRING>, f_bytes ARRAY<BYTES>>)] as f_nested)")


def test_json_complex_schema():
    transformer = JsonDataLiteralTransformer()
    query = transformer.load(
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/"
                          "complex_schema_datum.json"),
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/complex_schema.json")
    )
    assert query == ("(select 'f_string' as f_string, from_base64('YW55') as f_bytes, cast(1 as INT64) as f_int, "
                     "cast(1.1 as FLOAT64) as f_float, true as f_bool, "
                     "timestamp '2020-11-26 17:09:03.967259 UTC' as f_timestamp, date '2020-11-26' as f_date, "
                     "time '17:09:03.967259' as f_time, datetime '2020-11-26 17:09:03.967259' as f_datetime, "
                     "cast(1.2 as NUMERIC) as f_numeric, ST_GEOGPOINT(1, 2) as f_geography, "
                     "struct('f_string' as f_string, from_base64('YW55') as f_bytes, cast(2 as INT64) as f_int, "
                     "cast(2.1 as FLOAT64) as f_float, true as f_bool, "
                     "timestamp '2019-11-26 17:09:03.967259 UTC' as f_timestamp, date '2019-11-26' as f_date, "
                     "time '16:09:03.967259' as f_time, datetime '2019-11-26 17:09:03.967259' as f_datetime, "
                     "cast(2.2 as NUMERIC) as f_numeric, ST_GEOGPOINT(1, 2) as f_geography, "
                     "[struct('f_string' as f_string, from_base64('YW55') as f_bytes, "
                     "cast(1 as INT64) as f_int, cast(1.1 as FLOAT64) as f_float, true as f_bool, "
                     "timestamp '2020-11-26 17:09:03.967259 UTC' as f_timestamp, date '2020-11-26' as f_date, "
                     "time '17:09:03.967259' as f_time, datetime '2020-11-26 17:09:03.967259' as f_datetime, "
                     "cast(1.2 as NUMERIC) as f_numeric, ST_GEOGPOINT(1, 2) as f_geography)] as f_repeated_struct, "
                     "['f_string'] as f_string_repeated, [from_base64('YW55')] as f_bytes_repeated, "
                     "[cast(1 as INT64)] as f_int_repeated, [cast(1.1 as FLOAT64)] as f_float_repeated, "
                     "[true] as f_bool_repeated, [timestamp '2020-11-26 17:09:03.967259 UTC'] as f_timestamp_repeated,"
                     " [date '2020-11-26'] as f_date_repeated, [time '17:09:03.967259'] as f_time_repeated, "
                     "[datetime '2020-11-26 17:09:03.967259'] as f_datetime_repeated, "
                     "[cast(1.2 as NUMERIC)] as f_numeric_repeated, [ST_GEOGPOINT(1, 2)] as f_geography_repeated) "
                     "as f_struct, [struct('f_string' as f_string, from_base64('YW55') as f_bytes, "
                     "cast(1 as INT64) as f_int, cast(1.1 as FLOAT64) as f_float, true as f_bool, "
                     "timestamp '2020-11-26 17:09:03.967259 UTC' as f_timestamp, date '2020-11-26' as f_date, "
                     "time '17:09:03.967259' as f_time, datetime '2020-11-26 17:09:03.967259' as f_datetime, "
                     "cast(1.2 as NUMERIC) as f_numeric, ST_GEOGPOINT(1, 2) as f_geography)] as f_repeated_struct,"
                     " ['f_string'] as f_string_repeated, [from_base64('YW55')] as f_bytes_repeated, "
                     "[cast(1 as INT64)] as f_int_repeated, [cast(1.1 as FLOAT64)] as f_float_repeated, "
                     "[true] as f_bool_repeated, [timestamp '2020-11-26 17:09:03.967259 UTC'] as f_timestamp_repeated,"
                     " [date '2020-11-26'] as f_date_repeated, [time '17:09:03.967259'] as f_time_repeated, "
                     "[datetime '2020-11-26 17:09:03.967259'] as f_datetime_repeated, "
                     "[cast(1.2 as NUMERIC)] as f_numeric_repeated, [ST_GEOGPOINT(1, 2)] as f_geography_repeated"
                     "\nunion all\n"
                     "select cast(null as STRING) as f_string, cast(null as BYTES) as f_bytes, "
                     "cast(null as INT64) as f_int, cast(null as FLOAT64) as f_float, cast(null as BOOLEAN) as f_bool,"
                     " cast(null as TIMESTAMP) as f_timestamp, cast(null as DATE) as f_date, "
                     "cast(null as TIME) as f_time, cast(null as DATETIME) as f_datetime, "
                     "cast(null as NUMERIC) as f_numeric, cast(null as GEOGRAPHY) as f_geography, "
                     "cast(null as STRUCT<f_string STRING, f_bytes BYTES, f_int INT64, f_float FLOAT64, "
                     "f_bool BOOLEAN, f_timestamp TIMESTAMP, f_date DATE, f_time TIME, f_datetime DATETIME, "
                     "f_numeric NUMERIC, f_geography GEOGRAPHY, f_repeated_struct ARRAY<STRUCT<f_string STRING, "
                     "f_bytes BYTES, f_int INT64, f_float FLOAT64, f_bool BOOLEAN, f_timestamp TIMESTAMP, "
                     "f_date DATE, f_time TIME, f_datetime DATETIME, f_numeric NUMERIC, f_geography GEOGRAPHY>>, "
                     "f_string_repeated ARRAY<STRING>, f_bytes_repeated ARRAY<BYTES>, f_int_repeated ARRAY<INT64>, "
                     "f_float_repeated ARRAY<FLOAT64>, f_bool_repeated ARRAY<BOOLEAN>, "
                     "f_timestamp_repeated ARRAY<TIMESTAMP>, f_date_repeated ARRAY<DATE>, "
                     "f_time_repeated ARRAY<TIME>, f_datetime_repeated ARRAY<DATETIME>, "
                     "f_numeric_repeated ARRAY<NUMERIC>, f_geography_repeated ARRAY<GEOGRAPHY>>) as f_struct, "
                     "cast(null as ARRAY<STRUCT<f_string STRING, f_bytes BYTES, f_int INT64, f_float FLOAT64, "
                     "f_bool BOOLEAN, f_timestamp TIMESTAMP, f_date DATE, f_time TIME, f_datetime DATETIME, "
                     "f_numeric NUMERIC, f_geography GEOGRAPHY>>) as f_repeated_struct, "
                     "cast(null as ARRAY<STRING>) as f_string_repeated, "
                     "cast(null as ARRAY<BYTES>) as f_bytes_repeated, cast(null as ARRAY<INT64>) as f_int_repeated, "
                     "cast(null as ARRAY<FLOAT64>) as f_float_repeated, "
                     "cast(null as ARRAY<BOOLEAN>) as f_bool_repeated, "
                     "cast(null as ARRAY<TIMESTAMP>) as f_timestamp_repeated, "
                     "cast(null as ARRAY<DATE>) as f_date_repeated, "
                     "cast(null as ARRAY<TIME>) as f_time_repeated, "
                     "cast(null as ARRAY<DATETIME>) as f_datetime_repeated, "
                     "cast(null as ARRAY<NUMERIC>) as f_numeric_repeated, cast(null as ARRAY<GEOGRAPHY>) "
                     "as f_geography_repeated"
                     "\nunion all\n"
                     "select cast(null as STRING) as f_string, cast(null as BYTES) as f_bytes, "
                     "cast(null as INT64) as f_int, cast(null as FLOAT64) as f_float, cast(null as BOOLEAN) as f_bool,"
                     " cast(null as TIMESTAMP) as f_timestamp, cast(null as DATE) as f_date, "
                     "cast(null as TIME) as f_time, cast(null as DATETIME) as f_datetime, "
                     "cast(null as NUMERIC) as f_numeric, cast(null as GEOGRAPHY) as f_geography, "
                     "cast(null as STRUCT<f_string STRING, f_bytes BYTES, f_int INT64, f_float FLOAT64, "
                     "f_bool BOOLEAN, f_timestamp TIMESTAMP, f_date DATE, f_time TIME, f_datetime DATETIME, "
                     "f_numeric NUMERIC, f_geography GEOGRAPHY, f_repeated_struct ARRAY<STRUCT<f_string STRING, "
                     "f_bytes BYTES, f_int INT64, f_float FLOAT64, f_bool BOOLEAN, f_timestamp TIMESTAMP, f_date DATE,"
                     " f_time TIME, f_datetime DATETIME, f_numeric NUMERIC, f_geography GEOGRAPHY>>, "
                     "f_string_repeated ARRAY<STRING>, f_bytes_repeated ARRAY<BYTES>, f_int_repeated ARRAY<INT64>, "
                     "f_float_repeated ARRAY<FLOAT64>, f_bool_repeated ARRAY<BOOLEAN>, "
                     "f_timestamp_repeated ARRAY<TIMESTAMP>, f_date_repeated ARRAY<DATE>, "
                     "f_time_repeated ARRAY<TIME>, f_datetime_repeated ARRAY<DATETIME>, "
                     "f_numeric_repeated ARRAY<NUMERIC>, f_geography_repeated ARRAY<GEOGRAPHY>>) as f_struct, "
                     "[cast(null as STRUCT<f_string STRING, f_bytes BYTES, f_int INT64, f_float FLOAT64, "
                     "f_bool BOOLEAN, f_timestamp TIMESTAMP, f_date DATE, f_time TIME, f_datetime DATETIME, "
                     "f_numeric NUMERIC, f_geography GEOGRAPHY>)] as f_repeated_struct, "
                     "[cast(null as STRING)] as f_string_repeated, [cast(null as BYTES)] as f_bytes_repeated, "
                     "[cast(null as INT64)] as f_int_repeated, [cast(null as FLOAT64)] as f_float_repeated, "
                     "[cast(null as BOOLEAN)] as f_bool_repeated, [cast(null as TIMESTAMP)] as f_timestamp_repeated, "
                     "[cast(null as DATE)] as f_date_repeated, [cast(null as TIME)] as f_time_repeated, "
                     "[cast(null as DATETIME)] as f_datetime_repeated, [cast(null as NUMERIC)] as f_numeric_repeated, "
                     "[cast(null as GEOGRAPHY)] as f_geography_repeated)")


def test_json_empty_complex_schema():
    expected = ("(select * from (select cast(null as STRING) as f_string, cast(null as BYTES) as f_bytes, "
                "cast(null as INT64) as f_int, cast(null as FLOAT64) as f_float, cast(null as BOOLEAN) as f_bool,"
                " cast(null as TIMESTAMP) as f_timestamp, cast(null as DATE) as f_date, "
                "cast(null as TIME) as f_time, cast(null as DATETIME) as f_datetime, "
                "cast(null as NUMERIC) as f_numeric, cast(null as GEOGRAPHY) as f_geography, "
                "cast(null as STRUCT<f_string STRING, f_bytes BYTES, f_int INT64, f_float FLOAT64, "
                "f_bool BOOLEAN, f_timestamp TIMESTAMP, f_date DATE, f_time TIME, f_datetime DATETIME, "
                "f_numeric NUMERIC, f_geography GEOGRAPHY, f_repeated_struct ARRAY<STRUCT<f_string STRING, "
                "f_bytes BYTES, f_int INT64, f_float FLOAT64, f_bool BOOLEAN, f_timestamp TIMESTAMP, "
                "f_date DATE, f_time TIME, f_datetime DATETIME, f_numeric NUMERIC, f_geography GEOGRAPHY>>, "
                "f_string_repeated ARRAY<STRING>, f_bytes_repeated ARRAY<BYTES>, f_int_repeated ARRAY<INT64>, "
                "f_float_repeated ARRAY<FLOAT64>, f_bool_repeated ARRAY<BOOLEAN>, "
                "f_timestamp_repeated ARRAY<TIMESTAMP>, f_date_repeated ARRAY<DATE>, "
                "f_time_repeated ARRAY<TIME>, f_datetime_repeated ARRAY<DATETIME>, "
                "f_numeric_repeated ARRAY<NUMERIC>, f_geography_repeated ARRAY<GEOGRAPHY>>) as f_struct, "
                "cast(null as ARRAY<STRUCT<f_string STRING, f_bytes BYTES, f_int INT64, f_float FLOAT64, "
                "f_bool BOOLEAN, f_timestamp TIMESTAMP, f_date DATE, f_time TIME, f_datetime DATETIME, "
                "f_numeric NUMERIC, f_geography GEOGRAPHY>>) as f_repeated_struct, "
                "cast(null as ARRAY<STRING>) as f_string_repeated, "
                "cast(null as ARRAY<BYTES>) as f_bytes_repeated, cast(null as ARRAY<INT64>) as f_int_repeated, "
                "cast(null as ARRAY<FLOAT64>) as f_float_repeated, "
                "cast(null as ARRAY<BOOLEAN>) as f_bool_repeated, "
                "cast(null as ARRAY<TIMESTAMP>) as f_timestamp_repeated, "
                "cast(null as ARRAY<DATE>) as f_date_repeated, "
                "cast(null as ARRAY<TIME>) as f_time_repeated, "
                "cast(null as ARRAY<DATETIME>) as f_datetime_repeated, "
                "cast(null as ARRAY<NUMERIC>) as f_numeric_repeated, cast(null as ARRAY<GEOGRAPHY>) "
                "as f_geography_repeated) limit 0)")
    query = JsonDataLiteralTransformer().load(
        None,
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/complex_schema.json")
    )
    assert query == expected
    query = JsonDataLiteralTransformer(json_format=JsonFormat.JSON_ARRAY).load(
        "[]",
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/complex_schema.json")
    )
    assert query == expected
    query = JsonDataLiteralTransformer().load(
        [],
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/complex_schema.json")
    )
    assert query == expected


def test_required_schema():
    transformer = JsonDataLiteralTransformer()

    with pytest.raises(DataLiteralTransformException) as exception:
        transformer.load(
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/"
                              "required_schema_datum.json"),
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/required_schema.json")
        )
    assert str(exception.value) == ("Exception happened in line 1 with the following errors :\n"
                                    "\t.f_string is required,\n"
                                    "\t.f_bytes is required,\n"
                                    "\t.f_int is required,\n"
                                    "\t.f_float is required,\n"
                                    "\t.f_bool is required,\n"
                                    "\t.f_timestamp is required,\n"
                                    "\t.f_date is required,\n"
                                    "\t.f_time is required,\n"
                                    "\t.f_datetime is required,\n"
                                    "\t.f_numeric is required,\n"
                                    "\t.f_geography is required,\n"
                                    "\t.f_record is required,\n"
                                    "\t.f_repeated_record[0].f_string is required,\n"
                                    "\t.f_repeated_record[0].f_int is required,\n"
                                    "\t.f_repeated_record[0].f_bool is required,\n"
                                    "\t.f_repeated_record[0].f_date is required,\n"
                                    "\t.f_repeated_record[0].f_datetime is required,\n"
                                    "\t.f_repeated_record[0].f_geography is required,\n"
                                    "\t.f_repeated_record[0].f_record is required,\n"
                                    "\t.f_repeated_record[0].f_repeated_record[0].f_nested_string is required,\n"
                                    "\t.f_repeated_record[1].f_string is required,\n"
                                    "\t.f_repeated_record[1].f_int is required,\n"
                                    "\t.f_repeated_record[1].f_bool is required,\n"
                                    "\t.f_repeated_record[1].f_date is required,\n"
                                    "\t.f_repeated_record[1].f_datetime is required,\n"
                                    "\t.f_repeated_record[1].f_geography is required,\n"
                                    "\t.f_repeated_record[1].f_record is required,\n"
                                    "\t.f_repeated_record[1].f_repeated_record[0].f_nested_string is required"
                                    )


def test_geography_point():
    transformer = JsonDataLiteralTransformer()

    with pytest.raises(DataLiteralTransformException) as exception:
        transformer.load(
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/"
                              "geography_schema_datum.json"),
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/geography_schema.json")
        )
    assert str(exception.value) == ("Exception happened in line 6 with the following errors :\n"
                                    "\t.f_geography is a GEOGRAPHY type. It is expected to match POINT(x y) "
                                    "where x and y are FLOAT64. Instead get NotPoInT( -122.35  47.62 ). "
                                    "POINT is case insensitive.\n\n"
                                    "Exception happened in line 7 with the following errors :\n"
                                    "\t.f_geography is a GEOGRAPHY type. It is expected to match POINT(x y) "
                                    "where x and y are FLOAT64. Instead get NotPoInT( -122.35  47.62 ). "
                                    "POINT is case insensitive."
                                    )


def test_invalid_instance():
    transformer = JsonDataLiteralTransformer()
    with pytest.raises(InvalidInstanceException):
        transformer.load(
            123,
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/geography_schema.json")
        )
    with pytest.raises(InvalidInstanceException):
        transformer.with_json_format(JsonFormat.JSON_ARRAY).load(
            123,
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/geography_schema.json")
        )


def test_change_json_format():
    transformer = JsonDataLiteralTransformer()
    assert transformer.json_format == JsonFormat.NEWLINE_DELIMITED_JSON
    transformer = transformer.with_json_format(JsonFormat.JSON_ARRAY)
    assert transformer.json_format == JsonFormat.JSON_ARRAY


def test_json_load_as_simple_schema():
    transformer = JsonDataLiteralTransformer().use_datetime_like_cast()
    query = transformer.load_as({
        "input_1": (
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema_datum.json"),
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
        ),
        "input_2": (
            "{}",
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
        )
    })
    assert query == {
        "input_1": ("(select 'str with quote \\' and backslash \\\\' as f_string, "
                    "from_base64('YW55') as f_bytes, cast(1 as INT64) as f_int, cast(1.5 as FLOAT64) as f_float, "
                    "true as f_bool, "
                    "cast('2020-11-26 17:09:03.967259 UTC' as TIMESTAMP) as f_timestamp, "
                    "cast('2020-11-26' as DATE) as f_date, cast('11:09:03' as TIME) as f_time, "
                    "cast('2020-11-26T17:09:03' as DATETIME) as f_datetime, cast(1.6 as NUMERIC) as f_numeric, "
                    "ST_GEOGPOINT(-122.35, 47.62) as f_geography, "
                    "cast(null as STRUCT<f_datetime DATETIME>) as f_struct)"),
        "input_2": ("(select cast(null as STRING) as f_string, "
                    "cast(null as BYTES) as f_bytes, cast(null as INT64) as f_int, cast(null as FLOAT64) as f_float, "
                    "cast(null as BOOLEAN) as f_bool, "
                    "cast(null as TIMESTAMP) as f_timestamp, "
                    "cast(null as DATE) as f_date, cast(null as TIME) as f_time, "
                    "cast(null as DATETIME) as f_datetime, cast(null as NUMERIC) as f_numeric, "
                    "cast(null as GEOGRAPHY) as f_geography, "
                    "cast(null as STRUCT<f_datetime DATETIME>) as f_struct)")
    }


def test_json_empty_array_schema():
    expected = ("(select struct(cast(null as STRING) as f_string, "
                "cast([] as ARRAY<STRUCT<f_string STRING, f_bytes BYTES, f_int INT64, f_float FLOAT64, "
                "f_bool BOOLEAN, f_timestamp TIMESTAMP, f_date DATE, f_time TIME, f_datetime DATETIME, "
                "f_numeric NUMERIC, f_geography GEOGRAPHY>>) as f_repeated_struct, "
                "cast([] as ARRAY<STRING>) as f_string_repeated, cast([] as ARRAY<BYTES>) as f_bytes_repeated, "
                "cast([] as ARRAY<INT64>) as f_int_repeated, "
                "cast([] as ARRAY<FLOAT64>) as f_float_repeated, cast([] as ARRAY<BOOLEAN>) as f_bool_repeated, "
                "cast([] as ARRAY<TIMESTAMP>) as f_timestamp_repeated, cast([] as ARRAY<DATE>) as f_date_repeated, "
                "cast([] as ARRAY<TIME>) as f_time_repeated, cast([] as ARRAY<DATETIME>) as f_datetime_repeated, "
                "cast([] as ARRAY<NUMERIC>) as f_numeric_repeated, "
                "cast([] as ARRAY<GEOGRAPHY>) as f_geography_repeated) as f_struct, "
                "cast([] as ARRAY<STRUCT<f_string STRING, f_bytes BYTES, f_int INT64, f_float FLOAT64, "
                "f_bool BOOLEAN, f_timestamp TIMESTAMP, f_date DATE, f_time TIME, f_datetime DATETIME, "
                "f_numeric NUMERIC, f_geography GEOGRAPHY>>) as f_repeated_struct, "
                "cast([] as ARRAY<STRING>) as f_string_repeated, "
                "cast([] as ARRAY<BYTES>) as f_bytes_repeated, cast([] as ARRAY<INT64>) as f_int_repeated, "
                "cast([] as ARRAY<FLOAT64>) as f_float_repeated, "
                "cast([] as ARRAY<BOOLEAN>) as f_bool_repeated, "
                "cast([] as ARRAY<TIMESTAMP>) as f_timestamp_repeated, "
                "cast([] as ARRAY<DATE>) as f_date_repeated, "
                "cast([] as ARRAY<TIME>) as f_time_repeated, "
                "cast([] as ARRAY<DATETIME>) as f_datetime_repeated, "
                "cast([] as ARRAY<NUMERIC>) as f_numeric_repeated, cast([] as ARRAY<GEOGRAPHY>) "
                "as f_geography_repeated)")
    query = JsonDataLiteralTransformer(json_format=JsonFormat.JSON_ARRAY).load(
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/empty_array_schema_datum.json"),
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/empty_array_schema.json")
    )
    assert query == expected
