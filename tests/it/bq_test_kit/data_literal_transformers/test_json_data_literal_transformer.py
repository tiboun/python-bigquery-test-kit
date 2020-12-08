# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from bq_test_kit import BQTestKit
from bq_test_kit.bq_dsl.schema_mixin import SchemaMixin
from bq_test_kit.data_literal_transformers.json_data_literal_transformer import \
    JsonDataLiteralTransformer
from bq_test_kit.resource_loaders.package_file_loader import PackageFileLoader


def test_json_complex_schema(bqtk: BQTestKit):
    complex_schema_pfl = PackageFileLoader("tests/it/bq_test_kit/data_literal_transformers/resources/"
                                           "complex_schema.json")
    transformer = JsonDataLiteralTransformer()
    complex_schema = SchemaMixin().to_schema_field_list(complex_schema_pfl)
    query = transformer.load(
        PackageFileLoader("tests/it/bq_test_kit/data_literal_transformers/resources/complex_schema_datum.json"),
        complex_schema_pfl
    )
    result = bqtk.query_template(from_=query).run()

    assert result.schema == complex_schema


def test_json_complex_schema_with_cast(bqtk: BQTestKit):
    complex_schema_pfl = PackageFileLoader("tests/it/bq_test_kit/data_literal_transformers/resources/"
                                           "complex_schema.json")
    transformer = JsonDataLiteralTransformer().use_string_cast_to_bytes().use_datetime_like_cast()
    complex_schema = SchemaMixin().to_schema_field_list(complex_schema_pfl)
    query = transformer.load(
        PackageFileLoader("tests/it/bq_test_kit/data_literal_transformers/resources/complex_schema_datum.json"),
        complex_schema_pfl
    )
    result = bqtk.query_template(from_=query).run()

    assert result.schema == complex_schema
