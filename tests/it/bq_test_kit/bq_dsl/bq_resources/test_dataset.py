# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import os

import pytest
from google.api_core.exceptions import NotFound

from bq_test_kit import BQTestKit
from bq_test_kit.bq_dsl.bq_resources.resource_strategy import \
    CleanBeforeAndAfter


def test_isolate(bqtk: BQTestKit, bqtk_default_context: str):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        show_res = ds.show()
        assert show_res is not None
        assert show_res.location == "EU"
        expected_fqdn = (os.environ.get("GOOGLE_CLOUD_PROJECT") +
                         ":dataset_foo_" +
                         bqtk_default_context)
        assert show_res.full_dataset_id == expected_fqdn


def test_noop(bqtk: BQTestKit, bqtk_default_context: str):
    ds = bqtk.project("it").dataset(f"dataset_{bqtk_default_context}")
    with ds.noop() as ds:
        with pytest.raises(NotFound):
            ds.show()


def test_keep(bqtk: BQTestKit, bqtk_default_context: str):
    ds = bqtk.project("it").dataset(f"dataset_{bqtk_default_context}")
    ds = ds.clean_and_keep()
    with ds:
        assert ds.show() is not None
    assert ds.show() is not None
    ds.delete()


def test_clean_always(bqtk: BQTestKit, bqtk_default_context: str):
    ds = bqtk.project("it").dataset(f"dataset_{bqtk_default_context}")
    ds = ds.with_resource_strategy(CleanBeforeAndAfter())
    ds.create()
    assert ds.show() is not None
    with ds:
        assert ds.show() is not None
    with pytest.raises(NotFound):
        ds.show()


def test_change_create_options(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        assert ds.show() is not None
        create_options = {"exists_ok": True}
        with ds.with_create_options(**create_options) as ds2:
            show_res = ds2.show()
            assert show_res is not None
        with pytest.raises(NotFound):
            ds.show()


def test_location(bqtk: BQTestKit):
    ds = bqtk.project("it").dataset("dataset_foo").isolate()
    with ds.with_location("US") as ds_us:
        show_res = ds_us.show()
        assert show_res is not None
        assert show_res.location == "US"


def test_invalid_dataset_name(bqtk: BQTestKit):
    with pytest.raises(Exception):
        bqtk.project("it").dataset("dataset_#").isolate().create()
