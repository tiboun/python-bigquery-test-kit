# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest
from google.cloud.bigquery.schema import SchemaField

from bq_test_kit.data_literal_transformers import BaseDataLiteralTransformer


def test_load_implementation():
    bdlt = BaseDataLiteralTransformer()
    with pytest.raises(NotImplementedError):
        bdlt.load(1, [SchemaField("titi", "STRING")])
