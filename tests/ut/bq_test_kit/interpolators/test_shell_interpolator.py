# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from bq_test_kit.interpolators.shell_interpolator import ShellInterpolator


def test_interpolate():
    si = ShellInterpolator({"LOCAL_KEY": "VALUE"})
    result = si.interpolate("Local key has value ${LOCAL_KEY}."
                            " Global key has value ${GLOBAL_KEY}", {"GLOBAL_KEY": "G_VALUE"})
    assert result == ("Local key has value VALUE."
                      " Global key has value G_VALUE")
