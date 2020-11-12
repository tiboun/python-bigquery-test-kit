# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from datetime import date, datetime, time
from decimal import Decimal

import pytest
import pytz
from google.cloud.bigquery.query import ScalarQueryParameter
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Row

from bq_test_kit.bq_dsl.bq_resources.partitions import IngestionTime
from bq_test_kit.bq_test_kit import BQTestKit
from bq_test_kit.interpolators.jinja_interpolator import JinjaInterpolator
from bq_test_kit.interpolators.shell_interpolator import ShellInterpolator


def test_simple_query(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        schema = [SchemaField("f1", field_type="STRING")]
        with ds.table("table_bar", schema=schema).isolate() as t:
            rows = bqtk.query_template(from_=f"select count(*) as nb from `{t.fqdn()}`").run()
            assert len(rows) == 1
            assert rows[0]["nb"] == 0


def test_query_with_result_as_bqrow(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        schema = [SchemaField("f1", field_type="STRING")]
        with ds.table("table_bar", schema=schema).isolate() as t:
            rows = bqtk.query_template(from_=f"select count(*) as nb from `{t.fqdn()}`") \
                         .with_result_as_dict(False).run()
            assert len(rows) == 1
            assert isinstance(rows[0], Row)
            assert rows[0]["nb"] == 0


def test_query_with_destination(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        source_schema = [SchemaField("f1", field_type="STRING")]
        target_schema = [SchemaField("f1", field_type="STRING"), SchemaField("f2", field_type="INT64")]
        with ds.table("table_bar", schema=source_schema).isolate() as t_source:
            with ds.table("table_foobar", schema=target_schema).isolate() as t_target:
                query = f"select count(*) as f2, 'test' as f1 from `{t_source.fqdn()}`"
                rows_query = bqtk.query_template(from_=query).with_destination(t_target).run()
                assert len(rows_query) == 0
                rows_query = bqtk.query_template(from_="select 'test2' as f1, 2 as f2") \
                                 .with_destination(t_target).append().run()
                assert len(rows_query) == 0
                rows_target = bqtk.query_template(from_=f"select * from `{t_target.fqdn()}` order by f2").run()
                assert rows_target == [
                    {"f1": "test", "f2": 0},
                    {"f1": "test2", "f2": 2}
                ]


def test_query_with_destination_partition(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        target_schema = [SchemaField("f1", field_type="STRING"), SchemaField("f2", field_type="INT64")]
        with ds.table("table_foobar", schema=target_schema).isolate().partition_by(IngestionTime()) as t_target:
            bqtk.query_template(from_="select 'test2' as f1, 2 as f2") \
                                .with_destination(t_target, "20200101").append().run()
            rows_target = bqtk.query_template(from_="select *, _partitiondate as pd "
                                                    f"from `{t_target.fqdn()}` order by f2").run()
            assert rows_target == [
                {"f1": "test2", "f2": 2, "pd": date(2020, 1, 1)}
            ]


def test_query_with_parameters(bqtk: BQTestKit):
    rows = bqtk.query_template(from_="select @int_param as nb") \
                 .with_query_parameters([
                     ScalarQueryParameter("int_param", "INT64", 1)
                 ]) \
                 .run()
    assert len(rows) == 1
    assert rows[0]["nb"] == 1


def test_query_write_mode(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        source_schema = [SchemaField("f1", field_type="STRING")]
        target_schema = [SchemaField("f1", field_type="STRING"), SchemaField("f2", field_type="INT64")]
        with ds.table("table_bar", schema=source_schema).isolate() as t_source:
            with ds.table("table_foobar", schema=target_schema).isolate() as t_target:
                bqtk.query_template(from_=f"select count(*) as f2, 'test' as f1 from `{t_source.fqdn()}`") \
                    .with_destination(t_target).run()
                rows_target = bqtk.query_template(from_=f"select * from `{t_target.fqdn()}` order by f2").run()
                assert rows_target == [
                    {"f1": "test", "f2": 0}
                ]
                bqtk.query_template(from_="select 'test2' as f1, 2 as f2") \
                    .with_destination(t_target).append().run()
                rows_target = bqtk.query_template(from_=f"select * from `{t_target.fqdn()}` order by f2").run()
                assert rows_target == [
                    {"f1": "test", "f2": 0},
                    {"f1": "test2", "f2": 2}
                ]
                bqtk.query_template(from_="select 'test3' as f1, 3 as f2") \
                    .with_destination(t_target).overwrite().run()
                rows_target = bqtk.query_template(from_=f"select * from `{t_target.fqdn()}` order by f2").run()
                assert rows_target == [
                    {"f1": "test3", "f2": 3}
                ]
                with pytest.raises(Exception):
                    bqtk.query_template(from_="select 'test4' as f1, 4 as f2") \
                        .with_destination(t_target).error_if_exists().run()


def test_query_with_interpolators(bqtk: BQTestKit):
    rows = bqtk.query_template(from_="select ${NB_USER} as nb") \
                 .with_global_dict({"NB_USER": "2"}) \
                 .add_interpolator(ShellInterpolator()) \
                 .run()
    assert len(rows) == 1
    assert rows[0]["nb"] == 2
    rows = bqtk.query_template(from_="select {{NB_USER}} as nb") \
               .with_interpators([JinjaInterpolator({"NB_USER": "3"})]) \
               .run()
    assert len(rows) == 1
    assert rows[0]["nb"] == 3

    class DummyContainer():
        def get_nb_user(self):
            return 4
    rows = bqtk.query_template(from_="select {{container.get_nb_user()}} as nb") \
               .with_interpators([JinjaInterpolator({"container": DummyContainer()})]) \
               .run()
    assert len(rows) == 1
    assert rows[0]["nb"] == 4


def test_query_with_relative_table(bqtk: BQTestKit):
    with bqtk.project("it").dataset("dataset_foo").isolate() as ds:
        target_schema = [SchemaField("f1", field_type="STRING"), SchemaField("f2", field_type="INT64")]
        with ds.table("table_foobar", schema=target_schema).isolate().partition_by(IngestionTime()) as t_target:
            bqtk.query_template(from_="select 'test2' as f1, 2 as f2").with_destination(t_target).append().run()
            relative_name = ".".join(t_target.fqdn().split(".")[1:3])
            rows_target = bqtk.query_template(from_=f"select * from `{relative_name}` order by f2").run()
            assert rows_target == [
                {"f1": "test2", "f2": 2}
            ]


def test_query_all_bq_types(bqtk: BQTestKit):
    rows = bqtk.query_template(from_="""
        select
            'str' as f_string,
            1 as f_int64,
            cast(1.0 as NUMERIC)    as f_numeric,
            cast(1.1 as FLOAT64)    as f_float,
            false    as f_boolean,
            sha1('toto')    as f_bytes,
            Date('2020-11-22') as f_date,
            DateTime(2020, 11, 22, 23, 7, 59) as f_datetime,
            Time '23:08:04' as f_time,
            TIMESTAMP "2020-11-22 23:07:59 Europe/Paris" as f_timestamp,
            struct(sha1('toto') as f_bytes, [struct(sha1('toto') as f_bytes)] as f_array) as f_nested,
            [sha1('toto')] as f_array
    """).run()
    assert rows == [{
        "f_string": "str",
        "f_int64": 1,
        "f_numeric": Decimal(1.0),
        "f_float": 1.1,
        "f_boolean": False,
        "f_bytes": "C5wmJdwh7wX2rU3fR8XyA4N6oyw=",
        "f_date": date(2020, 11, 22),
        "f_datetime": datetime(2020, 11, 22, 23, 7, 59),
        "f_time": time(23, 8, 4),
        "f_timestamp": datetime(2020, 11, 22, 22, 7, 59, tzinfo=pytz.UTC),
        "f_nested": {
            "f_bytes": "C5wmJdwh7wX2rU3fR8XyA4N6oyw=",
            "f_array": [
                {
                    "f_bytes": "C5wmJdwh7wX2rU3fR8XyA4N6oyw="
                }
            ]
        },
        "f_array": ["C5wmJdwh7wX2rU3fR8XyA4N6oyw="]
    }]
