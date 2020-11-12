# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# C0114 disabled because this module contains only one class
# pylint: disable=C0114

from enum import Enum

from google.cloud.bigquery.table import \
    TimePartitioningType as BQTimePartitioningType


class TimePartitioningType(Enum):
    """Precision of time partitions.
    """
    DAY = BQTimePartitioningType.DAY
    HOUR = "HOUR"
    MONTH = "MONTH"
    YEAR = "YEAR"
