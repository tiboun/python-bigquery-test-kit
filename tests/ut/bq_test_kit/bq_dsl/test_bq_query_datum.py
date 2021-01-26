# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from typing import Any, Dict

from bq_test_kit.bq_dsl import BQQueryTemplate
from bq_test_kit.bq_dsl.bq_query_datum import BQQueryDatum
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.constants import DEFAULT_LOCATION
from bq_test_kit.data_literal_transformers.json_data_literal_transformer import \
    JsonDataLiteralTransformer
from bq_test_kit.interpolators.base_interpolator import BaseInterpolator
from bq_test_kit.resource_loaders import PackageFileLoader


class DummyInterpolator(BaseInterpolator):

    def interpolate(self, template: str, global_dict: Dict[str, Any]) -> str:
        return "rendered_" + template


def test_constructor():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    tables = {"table_one": ("{}", [])}
    bq_query_datum = BQQueryDatum(bq_tpl, True, tables)
    assert bq_query_datum.bq_query_template == bq_tpl
    assert bq_query_datum.use_temp_tables is True
    assert bq_query_datum.tables == tables


def test_switch_temp_table_usage():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    tables = {"table_one": ("{}", [])}
    bq_query_datum = BQQueryDatum(bq_tpl, False, tables)
    assert bq_query_datum.use_temp_tables is False
    bq_query_datum_2 = bq_query_datum.as_temp_tables()
    assert bq_query_datum.use_temp_tables is False
    assert bq_query_datum_2.use_temp_tables is True
    bq_query_datum_3 = bq_query_datum.as_data_literals()
    assert bq_query_datum.use_temp_tables is False
    assert bq_query_datum_2.use_temp_tables is True
    assert bq_query_datum_3.use_temp_tables is False


def test_temp_table_loaded_with():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    tables = {"table_one": ("{}", [])}
    bq_query_datum = BQQueryDatum(bq_tpl, True, tables)
    transformer = JsonDataLiteralTransformer()
    bq_tpl = bq_query_datum.loaded_with(transformer)
    assert bq_tpl.temp_tables == [(transformer, tables)]
    assert bq_tpl.global_dict == {}


def test_data_literal_loaded_with():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    tables = {"table_one": ("{}", [])}
    bq_query_datum = BQQueryDatum(bq_tpl, False, tables)
    transformer = JsonDataLiteralTransformer()
    bq_tpl = bq_query_datum.loaded_with(transformer)
    assert bq_tpl.temp_tables == []
    assert bq_tpl.global_dict == {"table_one": "(select cast(null as STRUCT<>))"}
