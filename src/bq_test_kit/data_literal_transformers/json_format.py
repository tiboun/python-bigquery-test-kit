# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from enum import Enum


class JsonFormat(Enum):
    """
        Specify how json data is structured inside the given string.
        NEWLINE_DELIMITED_JSON means that each line is a JSON element.
        JSON_ARRAY means that the whole input is a json_array.
    """
    NEWLINE_DELIMITED_JSON = "NEWLINE_DELIMITED_JSON"
    JSON_ARRAY = "JSON_ARRAY"
