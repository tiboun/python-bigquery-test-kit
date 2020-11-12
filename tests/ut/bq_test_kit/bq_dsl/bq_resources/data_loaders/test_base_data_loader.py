# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition

from bq_test_kit.bq_dsl.bq_resources.data_loaders.base_data_loader import \
    BaseDataLoader


def test_change_ignore_unknown_values():
    loader = BaseDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.ignore_unknown_values is None
    loader = loader.ignore_unknown_values()
    assert loader.load_job_config.ignore_unknown_values is True
    loader = loader.ignore_unknown_values(False)
    assert loader.load_job_config.ignore_unknown_values is False


def test_change_write_disposition():
    loader = BaseDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.load_job_config.write_disposition is None
    loader = loader.overwrite()
    assert loader.load_job_config.write_disposition == WriteDisposition.WRITE_TRUNCATE
    loader = loader.append()
    assert loader.load_job_config.write_disposition == WriteDisposition.WRITE_APPEND
    loader = loader.error_if_exists()
    assert loader.load_job_config.write_disposition == WriteDisposition.WRITE_EMPTY


def test_change_to_partition():
    loader = BaseDataLoader(table=None, from_=None, load_job_config=LoadJobConfig(), bq_client=None)
    assert loader.partition is None
    loader = loader.to_partition("20201023")
    assert loader.partition == "20201023"
