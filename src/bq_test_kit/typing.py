# Copyright (c) 2021 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

"""Register all alias here in order to make function signatures more readable.
"""

from typing import Dict, List, Optional, Tuple, Union

from google.cloud.bigquery.query import (ArrayQueryParameter,
                                         ScalarQueryParameter,
                                         StructQueryParameter)
from google.cloud.bigquery.schema import SchemaField

from bq_test_kit.resource_loaders.base_resource_loader import \
    BaseResourceLoader

DatumResource = Optional[Union[BaseResourceLoader, str, List[str]]]
SchemaResource = Union[BaseResourceLoader, str, List[SchemaField]]
TypedDatum = Tuple[DatumResource, SchemaResource]
SchemaFieldTypedDatum = Tuple[DatumResource, List[SchemaField]]
TableResources = Dict[str, TypedDatum]
QueryParameter = Union[ArrayQueryParameter, ScalarQueryParameter, StructQueryParameter]
