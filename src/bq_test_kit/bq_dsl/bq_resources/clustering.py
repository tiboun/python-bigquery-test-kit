from copy import deepcopy
from typing import List

from google.cloud.bigquery.table import Table as BQTable


class Clustering:
    """
    Defines the fields that a table should be clustered by.

    https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Clustering
    """
    def __init__(self, *fields: str):
        self.fields: List[str]
        if not fields:
            self.fields = []
        else:
            self.fields = fields

    def apply(self, bq_resource: BQTable) -> BQTable:
        if self.fields:
            new_resource = deepcopy(bq_resource)
            new_resource.clustering_fields = self.fields
            return new_resource
        else:
            return bq_resource

    def __repr__(self):
        return str(self.fields)
