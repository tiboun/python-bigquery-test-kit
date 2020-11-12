# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from copy import deepcopy
from typing import Any, Dict


class BaseInterpolator():
    """
        Base class for all interpolators
    """

    def __init__(self, local_dict: Dict[str, Any] = None) -> None:
        """Constructor of an interpolator

        Args:
            local_dict (Dict[str, Any], optional): dictionary specific to this interpolator. Defaults to None.
        """
        self.local_dict = local_dict if local_dict else {}

    def with_local_dict(self, local_dict: Dict[str, Any]):
        """Change local dict of the interpolator with the given one.

        Args:
            local_dict (Dict[str, Any]): local_dict to replace with.

        Returns:
            BaseInterpolator: new instance of the current interpolator
        """
        interpolator = deepcopy(self)
        interpolator.local_dict = local_dict if local_dict else {}
        return interpolator

    def merge_global_dict(self, global_dict: Dict[str, Any]) -> Dict[str, Any]:
        """merge global dictionary with the local one. Local dict takes precedence over the global one.

        Args:
            global_dict (Dict[str, Any]): global dictionary to mix with

        Returns:
            Dict[str, Any]: new dictionary where global and local dict are merged.
        """
        merged_dict = deepcopy(global_dict)
        merged_dict.update(self.local_dict)
        return merged_dict

    def interpolate(self, template: str, global_dict: Dict[str, Any]) -> str:
        """Interpolate the template using current interpolator with the global and local dictionary.

        Args:
            template (str): any string that should be interpolated.
            global_dict (Dict[str, Any]): global dictionary to mix with local one.

        Raises:
            NotImplementedError: Any interpolator must implement interpolate in order to be callable by the Query DSL.

        Returns:
            str: rendered template.
        """
        raise NotImplementedError("_interpolate must be defined in order to interpolate template."
                                  " Global dict can be merged with merge_global_dict.")
