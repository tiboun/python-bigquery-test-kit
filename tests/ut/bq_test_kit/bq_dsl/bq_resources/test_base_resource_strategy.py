# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest

from bq_test_kit.bq_dsl.bq_resources.resource_strategy import \
    BaseResourceStrategy


def test_exceptions():
    with pytest.raises(NotImplementedError):
        BaseResourceStrategy().before(None, None)
    with pytest.raises(NotImplementedError):
        BaseResourceStrategy().after(None)
