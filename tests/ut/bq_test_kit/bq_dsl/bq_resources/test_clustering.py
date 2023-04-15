from google.cloud.bigquery.table import Table as BQTable

from bq_test_kit.bq_dsl.bq_resources.clustering import Clustering


def test_apply_clustering():
    table = BQTable("project.dataset.table")
    assert table.clustering_fields is None
    table = Clustering().apply(table)
    # if you try to enable clustering with an empty field list, you'll get a BQ API error
    assert table.clustering_fields is None
    table = Clustering("cluster_field").apply(table)
    assert table.clustering_fields == ["cluster_field"]
    table = Clustering("field_a", "field_b").apply(table)
    assert table.clustering_fields == ["field_a", "field_b"]
