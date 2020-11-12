# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest
from google.api_core.exceptions import NotFound

from bq_test_kit.bq_dsl.bq_resources.tables import Tables
from bq_test_kit.bq_test_kit import BQTestKit


def test_resources_creation(bqtk: BQTestKit):
    p = bqtk.project("it") \
              .dataset("dataset_foo").isolate() \
              .table("table_foofoo") \
              .table("table_foobar") \
              .project.dataset("dataset_bar").isolate() \
              .table("table_barfoo") \
              .table("table_barbar") \
              .project
    table_foofoo, table_foobar, table_barfoo, table_barbar = None, None, None, None
    with Tables.from_(p, p) as tables:
        assert len(tables) == 4
        table_names = [t.name for t in tables]
        assert table_names == ["table_foofoo",
                               "table_foobar",
                               "table_barfoo",
                               "table_barbar"]
        table_foofoo, table_foobar, table_barfoo, table_barbar = tables
        assert table_foofoo.show() is not None
        assert table_foobar.show() is not None
        assert table_barfoo.show() is not None
        assert table_barbar.show() is not None
    with pytest.raises(NotFound):
        table_foofoo.show()
    with pytest.raises(NotFound):
        table_foobar.show()
    with pytest.raises(NotFound):
        table_barfoo.show()
    with pytest.raises(NotFound):
        table_barbar.show()
