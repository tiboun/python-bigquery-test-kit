# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from copy import deepcopy


class RawFileLoaderMixin():
    """This module provide features available for all raw file loader.
    """

    def with_encoding(self, encoding: str):
        """Set encoding of the file.

        Args:
            encoding (str): encoding of the file

        Returns:
            BaseDataLoader: return a new instance of the current type
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.encoding = encoding
        return data_loader

    def autodetect(self, active: bool = True):
        """Autodetect schema from

        Args:
            active (bool, optional): activate schema detection. Defaults to True.

        Returns:
            BaseDataLoader: return a new instance of the current type
        """
        data_loader = deepcopy(self)
        data_loader.load_job_config.autodetect = active
        return data_loader
