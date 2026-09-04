# Phase 5C ML Candidate Selection Validation

## Protocol

Models were fit on the train split only (409,590 rows per horizon) and scored
on the validation split only (108,336 rows per horizon), using the same
`paired_evaluation_eligible` population frozen in Phase 5B. Test partitions
(`target_local_month=11`/`12`) were never read; `feature_partition_paths`
raises if a test path is requested and the report records
`physical_test_month_partitions_read: []` and `test_read_scored_or_inspected:
false`. Horizons 1h, 3h, and 6h were trained and scored separately.

Feature set A is the approved demand-history and calendar predictors plus
Taxi Zone identity (`location_id`). Feature set B adds only the seven
approved weather predictors and their availability indicators. Categorical
columns (`location_id`, `target_local_hour`, `target_local_day_of_week`,
`target_local_month`, and `weather_code` in B) are indexed with
`StringIndexer(handleInvalid=keep)` then one-hot encoded
(`dropLast=false`, `handleInvalid=keep`); the indexer/encoder is fit on train
only and A/B share identical vocabularies for the four common categorical
columns. `location_id` is never cast to a numeric feature.

Two model families were evaluated with small predeclared validation-only
grids: regularized Linear Regression (3 configurations: `regParam` in
{0.01, 0.1, 1.0}, `elasticNetParam=0.0`) and Gradient-Boosted Trees
(2 configurations, seeded). All 12 model/feature-set/horizon combinations
produced 30 total grid fits. Raw predictions were clipped at zero
(`prediction = greatest(raw_prediction, 0)`) before computing metrics; this
nonnegative rule is identical and deterministic for every model. Validation
MAE is the primary selection metric, RMSE is secondary.

## Selected configuration by horizon

| Horizon | Model family | Feature set | Hyperparameters | Validation MAE | Validation RMSE | Baseline MAE | Improvement |
|---|---|---|---|---:|---:|---:|---:|
| 1h | Regularized Linear Regression | A | regParam=0.1, elasticNetParam=0.0 | 12.327 | 22.096 | 15.122 | 18.48% |
| 3h | Regularized Linear Regression | A | regParam=0.1, elasticNetParam=0.0 | 14.180 | 26.519 | 15.122 | 6.23% |
| 6h | Regularized Linear Regression | A | regParam=0.01, elasticNetParam=0.0 | 14.458 | 27.450 | 15.122 | 4.40% |

The selected model beats the frozen previous-week seasonal naive baseline
(validation MAE 15.122268) at every horizon; the improvement narrows as the
horizon lengthens. Baseline metrics were not recomputed and are reused
unchanged from Phase 5B.

## Weather A/B deltas (best configuration per model family and horizon)

| Horizon | Model family | A MAE | B MAE | Weather MAE change |
|---|---|---:|---:|---:|
| 1h | Regularized Linear Regression | 12.327 | 12.369 | -0.34% |
| 1h | Gradient-Boosted Trees | 13.597 | 13.621 | -0.18% |
| 3h | Regularized Linear Regression | 14.180 | 14.340 | -1.13% |
| 3h | Gradient-Boosted Trees | 14.547 | 14.655 | -0.74% |
| 6h | Regularized Linear Regression | 14.458 | 14.586 | -0.89% |
| 6h | Gradient-Boosted Trees | 14.777 | 14.835 | -0.39% |

A negative change means feature set B (with weather) had higher validation
MAE than feature set A at the identical row population and hyperparameter
configuration. Across all six model-family/horizon pairs, adding the
approved weather predictors did not improve validation MAE; feature set A
(demand/calendar only) was selected for all three horizons. This is an
observed result, not an assumed one.

## Integrity checks

- Zero test rows read, scored, or inspected (`test_rows_used: 0`).
- Preprocessing was fit on the train split only; validation was
  transform/evaluate only.
- Feature sets A and B share identical row populations per horizon
  (`A_B_population_key_difference: 0`; train 409,590 and validation 108,336
  rows for both A and B at every horizon) and identical categorical
  vocabularies for the four shared columns.
- Feature set B differs from A by weather columns only
  (`A_B_feature_difference_is_weather_only: true`).
- No duplicate `(location_id, target_time_utc, horizon_hours)` keys and zero
  forbidden (test-period) rows in the train/validation population.
- Every fit produced exactly the expected validation prediction count with
  zero null/NaN predictions (`prediction_count_mismatches: 0`).
- Raw negative predictions before clipping were reported per configuration
  (regularized Linear Regression: 2,824-4,117 rows across configurations;
  Gradient-Boosted Trees: 0-2 rows) and all were clipped to zero by the
  identical nonnegative rule prior to metric computation.
- `pickup_count` (the target) does not appear in either feature contract.
- Eight focused Spark tests validate train-only preprocessing fit and
  unseen-category handling, categorical zone treatment, A/B parity, no
  test-partition access, metric correctness after clipping, the identical
  nonnegative policy, MAE/RMSE-based validation selection, and the row key
  definition; all eight passed.

The machine-readable report is written to
`s3a://bigdata/silver/manifests/ml_candidate_selection_report.json` with a
local ignored copy at `data/silver/ml_candidate_selection_report.json`.
