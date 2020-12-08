import pytest

from bq_test_kit.data_literal_transformers.base_data_literal_transformer import \
    BaseDataLiteralTransformer


def test_load_implementation():
    bdlt = BaseDataLiteralTransformer()
    with pytest.raises(NotImplementedError):
        bdlt.load(None, None)
    with pytest.raises(NotImplementedError):
        bdlt.load_as(None)
