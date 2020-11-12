# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from typing import Any, Dict

from varsubst import varsubst
from varsubst.resolvers import DictResolver

from bq_test_kit.interpolators.base_interpolator import BaseInterpolator


class ShellInterpolator(BaseInterpolator):
    """Interpolate envsubst-like variables with the given dict. Please makes sur that variable used are of type str
       otherwise an exception may be thrown.
    """

    def interpolate(self, template: str, global_dict: Dict[str, Any]) -> str:
        merged_dict = self.merge_global_dict(global_dict)
        return varsubst(template, resolver=DictResolver(merged_dict))
