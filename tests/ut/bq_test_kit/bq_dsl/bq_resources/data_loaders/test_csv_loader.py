# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from google.cloud.bigquery.job import LoadJobConfig, SourceFormat

from bq_test_kit.bq_dsl.bq_resources.data_loaders import DsvDataLoader


def test_constructor():
    loader = DsvDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.table is None
    assert loader.from_ is None
    assert loader.load_job_config is not None
    assert loader.load_job_config.source_format == SourceFormat.CSV
    assert loader._bq_client is None


def test_change_allow_jagged_rules():
    loader = DsvDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.allow_jagged_rows is None
    loader = loader.allow_jagged_rows()
    assert loader.load_job_config.allow_jagged_rows is True
    loader = loader.allow_jagged_rows(False)
    assert loader.load_job_config.allow_jagged_rows is False


def test_change_allow_quoted_newlines():
    loader = DsvDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.allow_quoted_newlines is None
    loader = loader.allow_quoted_newlines()
    assert loader.load_job_config.allow_quoted_newlines is True
    loader = loader.allow_quoted_newlines(False)
    assert loader.load_job_config.allow_quoted_newlines is False


def test_change_encoding():
    loader = DsvDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.encoding is None
    loader = loader.with_encoding("UTF-8")
    assert loader.load_job_config.encoding == "UTF-8"


def test_change_autodetect():
    loader = DsvDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.autodetect is None
    loader = loader.autodetect()
    assert loader.load_job_config.autodetect is True
    loader = loader.autodetect(False)
    assert loader.load_job_config.autodetect is False


def test_change_field_delimiter():
    loader = DsvDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.field_delimiter is None
    loader = loader.with_field_delimiter("\t")
    assert loader.load_job_config.field_delimiter == "\t"


def test_change_null_marker():
    loader = DsvDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.null_marker is None
    loader = loader.with_null_marker("NULL")
    assert loader.load_job_config.null_marker == "NULL"


def test_change_quote_character():
    loader = DsvDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.quote_character is None
    loader = loader.with_quote_character("'")
    assert loader.load_job_config.quote_character == "'"


def test_change_skip_leading_rows():
    loader = DsvDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.skip_leading_rows is None
    loader = loader.skip_leading_rows(1)
    assert loader.load_job_config.skip_leading_rows == 1
