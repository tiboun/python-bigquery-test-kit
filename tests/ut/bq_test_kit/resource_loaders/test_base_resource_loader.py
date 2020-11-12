# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest

from bq_test_kit.resource_loaders.base_resource_loader import \
    BaseResourceLoader


def test_invalid_implementation():
    class InvalidResourceLoader(BaseResourceLoader):
        pass
    with pytest.raises(NotImplementedError):
        InvalidResourceLoader().load()
