# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest

from bq_test_kit.data_literal_transformers import DsvDataLiteralTransformer
from bq_test_kit.exceptions import (DataLiteralTransformException,
                                    InvalidInstanceException)
from bq_test_kit.resource_loaders.package_file_loader import PackageFileLoader


@pytest.mark.parametrize("json_input", [
    ('"str with quote \' and backslash \\\\","YW55",1,1.5,true,2020-11-26 17:09:03.967259 UTC,2020-11-26,11:09:03,'
     '2020-11-26T17:09:03,1.6,POINT(-122.35 47.62)'),
    ['"str with quote \' and backslash \\\\","YW55",1,1.5,true,2020-11-26 17:09:03.967259 UTC,2020-11-26,11:09:03,'
     '2020-11-26T17:09:03,1.6,POINT(-122.35 47.62)'],
    PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema_datum.csv")
 ])
def test_csv_load_simple_schema(json_input):
    transformer = DsvDataLiteralTransformer().use_datetime_like_cast()
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


def test_dsv_load_simple_schema_with_string_cast_to_bytes():
    transformer = DsvDataLiteralTransformer().use_string_cast_to_bytes().use_datetime_like_cast()
    query = transformer.load(
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema_datum.csv"),
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
    )
    assert query == ("(select 'str with quote \\' and backslash \\\\' as f_string, "
                     "cast('YW55' as BYTES) as f_bytes, cast(1 as INT64) as f_int, cast(1.5 as FLOAT64) as f_float, "
                     "true as f_bool, "
                     "cast('2020-11-26 17:09:03.967259 UTC' as TIMESTAMP) as f_timestamp, "
                     "cast('2020-11-26' as DATE) as f_date, cast('11:09:03' as TIME) as f_time, "
                     "cast('2020-11-26T17:09:03' as DATETIME) as f_datetime, cast(1.6 as NUMERIC) as f_numeric, "
                     "ST_GEOGPOINT(-122.35, 47.62) as f_geography, "
                     "cast(null as STRUCT<f_datetime DATETIME>) as f_struct)")


def test_dsv_load_with_extra_column():
    transformer = DsvDataLiteralTransformer()
    with pytest.raises(DataLiteralTransformException) as exception:
        transformer.load(
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/"
                              "simple_schema_with_extra_datum.csv"),
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
        )
    assert str(exception.value) == ("Exception happened in line 1 with the following errors :\n"
                                    "\tKey __extra-columns__ @ . not in schema\n\n"
                                    "Exception happened in line 2 with the following errors :\n"
                                    "\tKey __extra-columns__ @ . not in schema")
    query = transformer.ignore_unknown_values().use_datetime_like_cast().load(
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/"
                          "simple_schema_with_extra_datum.csv"),
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


def test_dsv_load_struct():
    transformer = DsvDataLiteralTransformer()
    with pytest.raises(DataLiteralTransformException) as exception:
        transformer.load(
            ('"other string","YW55",2,2.5,true,"2020-11-26 17:09:03.967259 UTC",'
             '"2020-11-26","11:09:03","2020-11-26 17:09:03",2.6,"POINT(-122.50 -40.6)",invalid struct'),
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
        )
    assert str(exception.value) == ("Exception happened in line 1 with the following errors :\n"
                                    "\t.f_struct is not a dictionary while schema is of type RECORD")


def test_tsv_load_simple_schema():
    transformer = DsvDataLiteralTransformer().use_datetime_like_cast() \
        .with_quote_character("#").with_field_delimiter("\t").with_escape_character("~")
    query = transformer.load(
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema_datum.tsv"),
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
    )
    assert query == ("(select 'str with quote \\' and backslash \\\\~' as f_string, "
                     "from_base64('YW55') as f_bytes, cast(1 as INT64) as f_int, cast(1.5 as FLOAT64) as f_float, "
                     "true as f_bool, "
                     "cast('2020-11-26 17:09:03.967259 UTC' as TIMESTAMP) as f_timestamp, "
                     "cast('2020-11-26' as DATE) as f_date, cast('11:09:03' as TIME) as f_time, "
                     "cast('2020-11-26T17:09:03' as DATETIME) as f_datetime, cast(1.6 as NUMERIC) as f_numeric, "
                     "ST_GEOGPOINT(-122.35, 47.62) as f_geography, "
                     "cast(null as STRUCT<f_datetime DATETIME>) as f_struct)")


def test_dsv_config():
    transformer = DsvDataLiteralTransformer().with_quote_character("#")\
        .with_field_delimiter("\t").with_escape_character("~").skip_leading_rows(1)
    assert transformer.quote_character == "#"
    assert transformer.field_delimiter == "\t"
    assert transformer.escape_character == "~"
    assert transformer.leading_rows_to_skip == 1


def test_csv_skip_line_load_simple_schema():
    transformer = DsvDataLiteralTransformer().use_datetime_like_cast().skip_leading_rows(1)
    query = transformer.load(
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema_datum.csv"),
        PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
    )
    assert query == ("(select * from (select cast(null as STRING) as f_string, cast(null as BYTES) as f_bytes, "
                     "cast(null as INT64) as f_int, cast(null as FLOAT64) as f_float, cast(null as BOOLEAN) as f_bool,"
                     " cast(null as TIMESTAMP) as f_timestamp, cast(null as DATE) as f_date, "
                     "cast(null as TIME) as f_time, cast(null as DATETIME) as f_datetime, "
                     "cast(null as NUMERIC) as f_numeric, cast(null as GEOGRAPHY) as f_geography, "
                     "cast(null as STRUCT<f_datetime DATETIME>) as f_struct) limit 0)")


def test_csv_invalid_load():
    transformer = DsvDataLiteralTransformer().use_datetime_like_cast().skip_leading_rows(1)
    with pytest.raises(InvalidInstanceException):
        transformer.load(
            123,
            PackageFileLoader("tests/ut/bq_test_kit/data_literal_transformers/resources/simple_schema.json")
        )
