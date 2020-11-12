# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from os.path import basename, dirname

import pkg_resources
from logzero import logger

from bq_test_kit.resource_loaders.base_resource_loader import \
    BaseResourceLoader


class PackageFileLoader(BaseResourceLoader):
    """Load from a given module path.
    """

    def __init__(self, path: str) -> None:
        """Path that is available in the module path.


        Args:
            path (str): relative module path like a path seperator '/' or '\\'.
        """
        self.path = path
        self.package = dirname(path).replace("/", ".").replace("\\", ".")
        self.file_name = basename(path)

    def load(self) -> str:
        logger.info("Loading file %s in package %s", self.file_name, self.package)
        with open(self.absolute_path(), 'r') as schemafile:
            return schemafile.read()

    def absolute_path(self) -> str:
        """
        Returns:
            str: return absolute path in order to read it.
        """
        return pkg_resources.resource_filename(self.package, self.file_name)

    def __repr__(self) -> str:
        return f"bq_test_kit.resource_loaders.PackageFileLoader('{self.path}')"

    def __str__(self) -> str:
        return self.__repr__()
