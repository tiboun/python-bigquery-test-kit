# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114


class BaseResourceLoader():
    """Base of all resource loader. Interface used by the DSL.
    """
    def load(self) -> str:
        """Retrieve from a resource and read it as a string.

        Raises:
            NotImplementedError: All loader must implement this method.

        Returns:
            str: resource as string.
        """
        raise NotImplementedError("Load must be implemented")
