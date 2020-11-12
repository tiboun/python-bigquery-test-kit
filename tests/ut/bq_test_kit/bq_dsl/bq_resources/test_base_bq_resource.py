# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest

from bq_test_kit.bq_dsl.bq_resources.base_bq_resource import BaseBQResource


def test_invalid_implementation():
    class InvalidBQResource(BaseBQResource):
        pass
    with pytest.raises(NotImplementedError):
        InvalidBQResource(name="r", bq_client=None, bqtk_config=None). \
            fqdn()
