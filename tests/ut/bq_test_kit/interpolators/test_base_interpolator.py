# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest

from bq_test_kit.interpolators.base_interpolator import BaseInterpolator


def test_interpolate_exception():
    with pytest.raises(NotImplementedError):
        BaseInterpolator().interpolate(template="tpl", global_dict={})


def test_merge_global_dict():
    bi = BaseInterpolator({"local": "dict", "override": "v2"})
    merge_result = bi.merge_global_dict({"override": "v1"})
    assert merge_result == {
        "local": "dict",
        "override": "v2"
    }


def test_change_local_dict():
    bi = BaseInterpolator({"local": "dict", "override": "v2"})
    bi = bi.with_local_dict({"override": "v1"})
    assert bi.local_dict == {"override": "v1"}
