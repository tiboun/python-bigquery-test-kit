# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from typing import Any, Dict

from jinja2.environment import Environment
from varsubst import varsubst
from varsubst.interpolators.jinja_interpolator import \
    JinjaInterpolator as VJinja
from varsubst.resolvers import DictResolver

from bq_test_kit.interpolators.base_interpolator import BaseInterpolator


class JinjaInterpolator(BaseInterpolator):
    """Interpolate with jinja template and benefits from all it's capabilities.
    """

    def __init__(self, local_dict: Dict[str, Any] = None, environment: Environment = None) -> None:
        """Constructor of JinjaInterpolator.

        Args:
            local_dict (Dict[str, Any]): local dictionary to use in this interpolator
            environment (Environment, optional): Jinja Environment used for interpolation.
                Useful if you plan to change anything like variable enclosures.
                Defaults to Environment().
        """
        super().__init__(local_dict)
        self.environment = environment if environment else Environment()

    def interpolate(self, template: str, global_dict: Dict[str, Any]) -> str:
        merged_dict = self.merge_global_dict(global_dict)
        return varsubst(template, interpolator=VJinja(environment=self.environment),
                        resolver=DictResolver(merged_dict))
