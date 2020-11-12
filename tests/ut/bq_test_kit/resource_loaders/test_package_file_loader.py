# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest

from bq_test_kit.resource_loaders import PackageFileLoader


def test_valid_path():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/resource_loaders/"
                            "resources/package_file_test_resource.txt")
    assert pfl.load() == "Loaded successfully"
    assert str(pfl) == ("bq_test_kit.resource_loaders.PackageFileLoader("
                        "'tests/ut/bq_test_kit/resource_loaders/"
                        "resources/package_file_test_resource.txt')")


def test_mixed_seperator():
    pfl = PackageFileLoader("tests\\ut/bq_test_kit\\resource_loaders/"
                            "resources/package_file_test_resource.txt")
    assert pfl.load() == "Loaded successfully"


def test_invalid_file_name():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/resource_loaders/"
                            "resources/package_file_test_resource_missing.txt")
    with pytest.raises(FileNotFoundError):
        pfl.load()


def test_invalid_package_name():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/resource_loaders/"
                            "missing_resources/package_file_test_resource.txt")
    with pytest.raises(ModuleNotFoundError):
        pfl.load()
