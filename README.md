[Big Query](https://cloud.google.com/bigquery) doesn't provide any locally runnabled server,
hence tests need to be run in Big Query itself.

BQ-test-kit enables Big Query testing by providing you an almost immutable DSL that allows you to :

 - create and delete dataset
 - create and delete table, partitioned or not
 - load csv or json data into tables
 - run query templates
 - transform json or csv data into a data literal

You can, therefore, test your query with data as literals or instantiate
datasets and tables in projects and load data into them.

It's faster to run query with data as literals but using materialized tables is mandatory for some use cases.

Immutability allows you to share datasets and tables definitions as a fixture and use it accros all tests,
adapt the definitions as necessary without worrying about mutations.

In order to have reproducible tests, BQ-test-kit add the ability to create isolated dataset or table,
thus query's outputs are predictable and assertion can be done in details.
If you are running simple queries (no DML), you can use data literal to make test running faster.

Template queries are rendered via [varsubst](https://pypi.org/project/varsubst/) but you can provide your own
interpolator by extending *bq_test_kit.interpolators.base_interpolator.BaseInterpolator*. Supported templates are
those supported by varsubst, namely envsubst-like (shell variables) or jinja powered.

In order to benefit from those interpolators, you will need to install one of the following extras,
**bq-test-kit[shell]** or **bq-test-kit[jinja2]**.

Usage
=====

Common
------

```python
from google.cloud.bigquery.client import Client
from bq_test_kit.bq_test_kit import BQTestKit
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.resource_loaders.package_file_loader import PackageFileLoader

client = Client(location="EU")
bqtk_conf = BQTestKitConfig().with_test_context("basic")
bqtk = BQTestKit(bq_client=client, bqtk_config=bqtk_conf)
# project() uses default one specified by GOOGLE_CLOUD_PROJECT environment variable
with bqtk.project().dataset("my_dataset").isolate().clean_and_keep() as d:
    # dataset `GOOGLE_CLOUD_PROJECT.my_dataset_basic` is created
    # isolation is done via isolate() and the given context.
    # if you are forced to use existing dataset, you must use noop().
    #
    # clean and keep will keep clean dataset if it exists before its creation.
    # Then my_dataset will be kept. This allows user to interact with BigQuery console afterwards.
    # Default behavior is to create and clean.
    schema = [SchemaField("f1", field_type="STRING"), SchemaField("f2", field_type="INT64")]
    with d.table("my_table", schema=schema).clean_and_keep() as t:
        # table `GOOGLE_CLOUD_PROJECT.my_dataset_basic.my_table` is created
        # noop() and isolate() are also supported for tables.
        pfl = PackageFileLoader("tests/it/bq_test_kit/bq_dsl/bq_resources/data_loaders/resources/dummy_data.csv")
        t.csv_loader(from_=pfl).load()
        result = bqtk.query_template(from_=f"select count(*) as nb from `{t.fqdn()}`").run()
        assert len(result.rows) > 0
    # table `GOOGLE_CLOUD_PROJECT.my_dataset_basic.my_table` is deleted
# dataset `GOOGLE_CLOUD_PROJECT.my_dataset_basic` is deleted
```

Advanced
--------

```python
from google.cloud.bigquery.client import Client
from bq_test_kit.bq_test_kit import BQTestKit
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.resource_loaders.package_file_loader import PackageFileLoader

client = Client(location="EU")
bqtk_conf = BQTestKitConfig().with_test_context("basic")
bqtk = BQTestKit(bq_client=client, bqtk_config=bqtk_conf)
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
    # create datasets and tables in the order built with the dsl. Then, a tuples of all tables are returned.
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
```

Simple query
--------

```python

import pytz

from datetime import datetime
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.schema import SchemaField
from bq_test_kit.bq_test_kit import BQTestKit
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.data_literal_transformers.json_data_literal_transformer import JsonDataLiteralTransformer
from bq_test_kit.interpolators.shell_interpolator import ShellInterpolator

client = Client(location="EU")
bqtk_conf = BQTestKitConfig().with_test_context("basic")
bqtk = BQTestKit(bq_client=client, bqtk_config=bqtk_conf)
data_literals_dict = JsonDataLiteralTransformer().load_as({
    "TABLE_FOO": (['{"foobar": "1", "foo": 1, "_PARTITIONTIME": "2020-11-26 17:09:03.967259 UTC"}'], [SchemaField("foobar", "STRING"), SchemaField("foo", "INT64"), SchemaField("_PARTITIONTIME", "TIMESTAMP")]),
    "TABLE_BAR": (['{"foobar": "1", "bar": 2}'], [SchemaField("foobar", "STRING"), SchemaField("bar", "INT64")]),
    "TABLE_EMPTY": (None, [SchemaField("foobar", "STRING"), SchemaField("baz", "INT64")])
})
results = bqtk.query_template(from_="""
    SELECT
        f.foo, b.bar, e.baz, f._partitiontime as pt
    FROM
        ${TABLE_FOO} f
        INNER JOIN ${TABLE_BAR} b ON f.foobar = b.foobar
        LEFT JOIN ${TABLE_EMPTY} e ON b.foobar = e.foobar
""").update_global_dict(data_literals_dict).add_interpolator(ShellInterpolator()).run()
assert len(results.rows) == 1
assert results.rows == [{"foo": 1, "bar": 2, "baz": None, "pt": datetime(2020, 11, 26, 17, 9, 3, 967259, pytz.UTC)}]
```

More usage can be found in [it tests](https://github.com/tiboun/python-bq-test-kit/tree/main/tests/it).

Concepts
========

Resource Loaders
----------------

Currently, the only resource loader available is *bq_test_kit.resource_loaders.package_file_loader.PackageFileLoader*.
It allows you to load a file from a package, so you can load any file from your source code.

You can implement yours by extending *bq_test_kit.resource_loaders.base_resource_loader.BaseResourceLoader*.
If so, please create a merge request if you think that yours may be interesting for others.

Interpolators
-------------

Interpolators enable variable substitution within a template.
They lay on dictionaries which can be in a global scope or interpolator scope.
While rendering template, interpolator scope's dictionary is merged into global scope thus,
interpolator scope takes precedence over global one.

You can benefit from two interpolators by installing the extras **bq-test-kit[shell]** or **bq-test-kit[jinja2]**.
Those extra allows you to render you query templates with envsubst-like variable or jinja.
You can define yours by extending *bq_test_kit.interpolators.BaseInterpolator*.


```python
from google.cloud.bigquery.client import Client
from bq_test_kit.bq_test_kit import BQTestKit
from bq_test_kit.bq_test_kit_config import BQTestKitConfig
from bq_test_kit.resource_loaders.package_file_loader import PackageFileLoader
from bq_test_kit.interpolators.jinja_interpolator import JinjaInterpolator
from bq_test_kit.interpolators.shell_interpolator import ShellInterpolator

client = Client(location="EU")
bqtk_conf = BQTestKitConfig().with_test_context("basic")
bqtk = BQTestKit(bq_client=client, bqtk_config=bqtk_conf)

result = bqtk.query_template(from_="select ${NB_USER} as nb") \
             .with_global_dict({"NB_USER": "2"}) \
             .add_renderer(ShellInterpolator()) \
             .run()
assert len(result.rows) == 1
assert result.rows[0]["nb"] == 2
result = bqtk.query_template(from_="select {{NB_USER}} as nb") \
             .with_renderers([JinjaInterpolator({"NB_USER": "3"})]) \
             .run()
assert len(result.rows) == 1
assert result.rows[0]["nb"] == 3
```


Data loaders
------------

Supported data loaders are csv and json only even if Big Query API support more.
Data loaders were restricted to those because they can be easily modified by a human and are maintainable.

If you need to support more, you can still load data by instantiating
*bq_test_kit.bq_dsl.bq_resources.data_loaders.base_data_loader.BaseDataLoader*.


Data Literal Transformers
-------------------------

Supported data literal transformers are csv and json.
Data Literal Transformers can be less strict than their counter part, Data Loaders.
In fact, they allow to use cast technique to transform string to bytes or cast a date like to its target type.
Furthermore, in json, another format is allowed, JSON_ARRAY.
This allows to have a better maintainability of the test resources.

Data Literal Transformers allows you to specify _partitiontime or _partitiondate as well,
thus you can specify all your data in one file and still matching the native table behavior.
If you were using Data Loader to load into an ingestion time partitioned table,
you would have to load data into specific partition.
Loading into a specific partition make the time rounded to 00:00:00.

If you need to support a custom format, you may extend BaseDataLiteralTransformer
to benefit from the implemented data literal conversion.
*bq_test_kit.data_literal_transformers.base_data_literal_transformer.BaseDataLiteralTransformer*.

Resource strategies
-------------------

Dataset and table resource management can be changed with one of the following :
 - Noop : don't manage the resource at all
 - CleanBeforeAndAfter : clean before each creation and after each usage.
 - CleanAfter : create without cleaning first and delete after each usage. This is the default behavior.
 - CleanBeforeAndKeepAfter : clean before each creation and don't clean resource after each usage.

The DSL on dataset and table scope provides the following methods in order to change resource strategy :
 - noop : set to Noop
 - clean_and_keep : set to CleanBeforeAndKeepAfter
 - with_resource_strategy : set to any resource strategy you want

Contributions
=============

Contributions are welcome. You can create issue to share a bug or an idea.
You can create merge request as well in order to enhance this project.

Local testing
-------------

Testing is separated in two parts :

 - unit testing : doesn't need interaction with Big Query
 - integration testing : validate behavior against Big Query

In order to run test locally, you must install [tox](https://pypi.org/project/tox/).

After that, you are able to run unit testing with `tox -e clean, py36-ut` from the root folder.

If you plan to run integration testing as well, please use a service account and authenticate yourself with `gcloud auth application-default login` which will set **GOOGLE_APPLICATION_CREDENTIALS** env var. You will have to set **GOOGLE_CLOUD_PROJECT** env var as well in order to run `tox`.

Thanks.


WARNING
=======

DSL may change with breaking change until release of 1.0.0.

Editing in VSCode
=================

In order to benefit from VSCode features such as debugging, you should type the following commands in the root folder of this project.

Linux
-----

 - python3 -m venv .venv
 - source .venv/bin/activate
 - pip3 install -r requirements.txt -r requirements-test.txt -e .

Windows
-------

 - py -3 -m venv .venv
 - .venv\scripts\activate
 - python -m pip install -r requirements.txt -r requirements-test.txt -e .

Changelog
=========

0.2.0
-----

 - Change return type of `Tables.__enter__` (closes #6)
 - Make jinja's local dictionary optional (closes #7)
 - Wrap query result into BQQueryResult (closes #9)


0.1.2
-----

 - Fix time partitioning type in TimeField (closes #3)

0.1.1
-----

 - Fix table reference in Dataset (closes #2)

0.1.0
-----

 - BigQuery resource DSL to create dataset and table (partitioned or not)
 - isolation of dataset and table names
 - results as dict with ease of test on byte arrays. Decoded as base64 string.
 - query templates interpolation made easy
 - csv and json loading into tables, including partitioned one, from code based resources.
 - context manager for cascading creation of BQResource
 - resource definition sharing accross tests made possible with "immutability".
