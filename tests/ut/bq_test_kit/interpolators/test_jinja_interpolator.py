# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from bq_test_kit.interpolators.jinja_interpolator import JinjaInterpolator


def test_interpolate():
    ji = JinjaInterpolator({"LOCAL_KEY": "VALUE"})
    result = ji.interpolate("Local key has value {{LOCAL_KEY}}."
                            " Global key has value {{GLOBAL_KEY}}", {"GLOBAL_KEY": "G_VALUE"})
    assert result == ("Local key has value VALUE."
                      " Global key has value G_VALUE")
