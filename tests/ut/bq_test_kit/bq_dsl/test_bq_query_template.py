# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from typing import Any, Dict

from google.cloud.bigquery.job import WriteDisposition
from google.cloud.bigquery.query import ScalarQueryParameter, UDFResource

from bq_test_kit.bq_dsl import BQQueryTemplate, Dataset, Project, Table
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.constants import DEFAULT_LOCATION
from bq_test_kit.interpolators.base_interpolator import BaseInterpolator
from bq_test_kit.resource_loaders import PackageFileLoader


class DummyInterpolator(BaseInterpolator):

    def interpolate(self, template: str, global_dict: Dict[str, Any]) -> str:
        return "rendered_" + template


def test_constructor():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    assert isinstance(bq_tpl.from_, PackageFileLoader)
    assert bq_tpl._bq_client is None
    assert bq_tpl.result_as_dict is True
    assert bq_tpl.job_config is not None
    assert bq_tpl.location == "EU"
    assert bq_tpl.project is None
    assert bq_tpl.interpolators == []


def test_change_allow_large_result():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    assert bq_tpl.job_config.allow_large_results is None
    bq_tpl = bq_tpl.allow_large_results(True)
    assert bq_tpl.job_config.allow_large_results is True


def test_change_destination():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", dataset=ds, bq_client=None, bqtk_config=conf)
    assert bq_tpl.job_config.destination is None
    bq_tpl = bq_tpl.with_destination(table)
    assert bq_tpl.job_config.destination.table_id == "table_bar"


def test_change_query_parameters():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    assert bq_tpl.job_config.query_parameters == []
    params = [ScalarQueryParameter("p1", "STRING", "v1")]
    bq_tpl = bq_tpl.with_query_parameters(params)
    assert bq_tpl.job_config.query_parameters is not None


def test_change_udf_resources():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    assert bq_tpl.job_config.udf_resources == []
    udf = UDFResource("inlineCode", "var someCode = 'here';")
    bq_tpl = bq_tpl.add_udf_resource(udf)
    assert len(bq_tpl.job_config.udf_resources) == 1
    bq_tpl = bq_tpl.add_udf_resource(udf)
    assert len(bq_tpl.job_config.udf_resources) == 2
    bq_tpl = bq_tpl.with_udf_resources([udf])
    assert len(bq_tpl.job_config.udf_resources) == 1


def test_change_legacy_sql():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    assert bq_tpl.job_config.use_legacy_sql is None
    bq_tpl = bq_tpl.use_legacy_sql(True)
    assert bq_tpl.job_config.use_legacy_sql is True


def test_change_query_cache():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    assert bq_tpl.job_config.use_query_cache is None
    bq_tpl = bq_tpl.use_query_cache(True)
    assert bq_tpl.job_config.use_query_cache is True


def test_change_result_as_dict():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    assert bq_tpl.result_as_dict is True
    bq_tpl = bq_tpl.with_result_as_dict(False)
    assert bq_tpl.result_as_dict is False


def test_change_write_disposition():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    assert bq_tpl.job_config.write_disposition is None
    bq_tpl = bq_tpl.overwrite()
    assert bq_tpl.job_config.write_disposition == WriteDisposition.WRITE_TRUNCATE
    bq_tpl = bq_tpl.append()
    assert bq_tpl.job_config.write_disposition == WriteDisposition.WRITE_APPEND
    bq_tpl = bq_tpl.error_if_exists()
    assert bq_tpl.job_config.write_disposition == WriteDisposition.WRITE_EMPTY


def test_change_interpolators():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None)
    assert bq_tpl.interpolators == []
    bq_tpl = bq_tpl.add_interpolator(DummyInterpolator())
    assert len(bq_tpl.interpolators) == 1
    bq_tpl = bq_tpl.add_interpolator(DummyInterpolator())
    assert len(bq_tpl.interpolators) == 2
    bq_tpl = bq_tpl.with_interpators([DummyInterpolator()])
    assert len(bq_tpl.interpolators) == 1


def test_render():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None, interpolators=[DummyInterpolator()])
    assert bq_tpl._interpolate() == "rendered_select * from my_table\n"


def test_global_dict():
    pfl = PackageFileLoader("tests/ut/bq_test_kit/bq_dsl/resources/dummy_query.sql")
    conf = BQTestKitConfig({DEFAULT_LOCATION: "EU"})
    bq_tpl = BQQueryTemplate(from_=pfl, bqtk_config=conf, bq_client=None, interpolators=[DummyInterpolator()])
    project = Project("test_project", bq_client=None, bqtk_config=conf)
    ds = Dataset("dataset_foo", project=project, bq_client=None,
                 bqtk_config=conf)
    table = Table("table_bar", dataset=ds, bq_client=None, bqtk_config=conf)
    table_with_alias = Table("table_bar", dataset=ds, bq_client=None, bqtk_config=conf).with_alias("table_foobar")
    bq_tpl = bq_tpl.with_global_dict({"test_project": "unknown"})
    assert bq_tpl.global_dict == {"test_project": "unknown"}
    bq_tpl = bq_tpl.update_global_dict([table, ds, project, table_with_alias])
    assert bq_tpl.global_dict == {
                                    "test_project": project.fqdn(),
                                    "dataset_foo": ds.fqdn(),
                                    "dataset_foo_table_bar": table.fqdn(),
                                    "table_foobar": table_with_alias.fqdn()
                                 }
    bq_tpl = bq_tpl.update_global_dict({"dataset_foo_table_bar": "my_override",
                                        "new_complex_key": {"new_key": "new_value"}})
    assert bq_tpl.global_dict == {
                                    "test_project": project.fqdn(),
                                    "dataset_foo": ds.fqdn(),
                                    "dataset_foo_table_bar": "my_override",
                                    "table_foobar": table_with_alias.fqdn(),
                                    "new_complex_key": {"new_key": "new_value"}
                                 }
