# Modeling Features Validation

## Scope and output

The Phase 5A Spark job reads only the validated Silver demand-weather join and
hourly demand grid. Of 6,885,360 Silver join rows, 4,940,640 belong to zones
outside the approved 74-zone set. The approved set contains 1,944,720 candidate
rows; 444 targets are explicitly unavailable at the unresolved fall-back hour.
The persisted feature dataset therefore contains 1,944,276 rows with no other
exclusions: 648,092 rows for each of the 1h, 3h, and 6h horizons. Every selected
zone contributes 26,274 rows.

The Parquet output is partitioned by horizon and local month at
`s3a://bigdata/silver/modeling_features/records`. Its inventory contains 37
objects totaling 90,690,988 bytes, including the zero-byte `_SUCCESS` marker.
The machine-readable report is
`s3a://bigdata/silver/manifests/modeling_features_report.json`.

## Eligibility and missingness

No eligible-target row was silently dropped and no feature was imputed. Missing
demand-history counts are:

| Feature | Rows missing |
|---|---:|
| Demand at cutoff | 1,110 |
| Demand 1h before cutoff | 1,406 |
| Demand 2h before cutoff | 1,628 |
| Same local target hour previous day | 5,772 |
| Same local target hour previous week | 37,740 |
| Trailing mean, 3h | 1,998 |
| Trailing mean, 6h | 3,330 |
| Trailing mean, 24h | 11,322 |
| Trailing population standard deviation, 24h | 11,322 |

Overall, 1,900,838 rows have complete demand history, 27,972 rows have at least
one weather predictor missing, and 1,872,866 rows have both complete demand
history and complete weather predictors. The latter is recorded as a flag for
later paired evaluation; no train-time missing-data policy is selected here.

## Leakage and integrity checks

Feature set A has nine demand-history and four calendar predictors. Feature set
B contains the identical A columns plus only the seven approved forecast
variables and their missingness/source-availability indicators. Target demand
is in neither feature definition.

The full run found zero source timestamps after prediction cutoff, zero rolling
windows ending anywhere except the cutoff, and zero duplicate
`(location_id, target_time_utc, horizon_hours)` keys. Six synthetic Spark tests
also verified cutoff timing, rolling boundaries and values, local previous-day
and previous-week behavior, zone isolation, feature-set parity, DST calendar
conversion, weather-missing preservation, and unique keys.
