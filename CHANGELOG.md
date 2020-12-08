## v0.2.0 (2020-12-01)

### Feat

- **bq_dsl**: make jinja interpolator's local dict optional
- **bq_dsl**: return precised type of Tuples in Tables.__enter__
- **bq_query**: enhance result of the bq query result

## v0.1.2 (2020-11-30)

### Fix

- **bq_dsl**: fix time partitioning type in TimeField

## v0.1.1 (2020-11-30)

### Fix

- **bq_dsl**: update table reference in dataset tables

## v0.1.0 (2020-11-24)

### Feat

- **bq_dsl**: BigQuery resource DSL to create dataset and table (partitioned or not)
- **bq_dsl**: isolation of dataset and table names
- **bq_dsl**: results as dict with ease of test on byte arrays. Decoded as base64 string.
- **bq_dsl**: query templates interpolation made easy
- **bq_dsl**: csv and json loading into tables, including partitioned one, from code based resources.
- **bq_dsl**: context manager for cascading creation of BQResource
- **bq_dsl**: resource definition sharing accross tests made possible with "immutability".
