# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from google.cloud.bigquery.job import LoadJobConfig, SourceFormat

from bq_test_kit.bq_dsl.bq_resources.data_loaders import JsonDataLoader


def test_constructor():
    loader = JsonDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.table is None
    assert loader.from_ is None
    assert loader.load_job_config is not None
    assert loader.load_job_config.source_format == SourceFormat.NEWLINE_DELIMITED_JSON
    assert loader._bq_client is None


def test_change_encoding():
    loader = JsonDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.encoding is None
    loader = loader.with_encoding("UTF-8")
    assert loader.load_job_config.encoding == "UTF-8"


def test_change_autodetect():
    loader = JsonDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.autodetect is None
    loader = loader.autodetect()
    assert loader.load_job_config.autodetect is True
    loader = loader.autodetect(False)
    assert loader.load_job_config.autodetect is False
