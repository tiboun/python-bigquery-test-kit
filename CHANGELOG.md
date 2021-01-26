## v0.4.1 (2021-01-26)

### Fix

- fix empty array data literal

## v0.4.0 (2021-01-26)

### Fix

- generate empty data literal when json array is empty

### Feat

- use temp tables or data literals in query template

## v0.3.1 (2020-12-17)

### Fix

- change method typo of with_interpolators

## v0.3.0 (2020-12-17)

### Refactor

- add data literal transformer package exports

### Feat

- log load job id
- add csv literal transformer
- add json data literal transformer

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

- **test-kit**: add ability to create isolated bq resources and query them
