# Phase 5D Final Frozen TEST Evaluation Validation

## Protocol

The Phase 5C-selected model family (Regularized Linear Regression) and its
per-horizon, per-feature-set hyperparameters were frozen from
`data/silver/ml_candidate_selection_report.json` *before* any TEST access:
`regParam=0.1` (1h, A and B), `regParam=0.1` (3h, A and B), `regParam=0.01`
(6h, A and B); `elasticNetParam=0.0` and `maxIter=50` at every horizon.
Gradient-Boosted Trees was not evaluated on TEST because it was not selected.
The frozen previous-week seasonal naive baseline test metrics were reused
unchanged from `data/silver/chronological_splits_baselines_report.json`
(Phase 5B) and not recomputed.

For each horizon (1h/3h/6h) and feature set (A: demand/calendar + Taxi Zone;
B: A plus approved weather), preprocessing (`StringIndexer` +
`OneHotEncoder` + `VectorAssembler`, identical definition to Phase 5C) and
the frozen Regularized Linear Regression configuration were refit on
TRAIN+VALIDATION only (517,926 rows per horizon), then scored once on the
frozen TEST partitions (`target_local_month=11,12`). The identical
zero-clipping nonnegative rule (`prediction = greatest(raw_prediction, 0)`)
was applied before computing metrics. No hyperparameter, feature, model
family, or preprocessing choice was changed after observing TEST results.

## Final TEST metrics by horizon and feature set

| Horizon | Feature set | Rows | MAE | RMSE | Raw negative predictions | Clipped predictions |
|---|---|---:|---:|---:|---:|---:|
| 1h | A | 106,412 | 13.789029 | 24.889088 | 2,838 | 2,838 |
| 1h | B | 106,412 | 13.775351 | 24.882623 | 3,746 | 3,746 |
| 3h | A | 106,338 | 17.476389 | 33.478442 | 3,335 | 3,335 |
| 3h | B | 106,338 | 17.474982 | 33.467007 | 4,575 | 4,575 |
| 6h | A | 106,338 | 18.350151 | 35.703538 | 2,897 | 2,897 |
| 6h | B | 106,338 | 18.369958 | 35.706288 | 4,085 | 4,085 |

## Weather A/B TEST deltas (Regularized Linear Regression)

| Horizon | A MAE | B MAE | Absolute delta | Percent delta |
|---|---:|---:|---:|---:|
| 1h | 13.789029 | 13.775351 | 0.013679 | +0.099% |
| 3h | 17.476389 | 17.474982 | 0.001407 | +0.008% |
| 6h | 18.350151 | 18.369958 | -0.019807 | -0.108% |

A positive delta means feature set B (with weather) had lower TEST MAE than
A. On the frozen TEST partitions, weather's measured effect is a small
positive delta at 1h, a negligible delta at 3h, and a small negative delta
at 6h — all under 0.11% in magnitude. This does not reverse the Phase 5C
validation finding that weather did not materially improve accuracy; the
effect remains close to zero and changes sign across horizons.

## Comparison to the frozen selected baseline (Phase 5B, previous-week seasonal naive)

| Horizon | Baseline test MAE | Baseline test RMSE | Selected A test MAE | MAE improvement |
|---|---:|---:|---:|---:|
| 1h | 20.969355 | 43.094060 | 13.789029 | 7.180326 |
| 3h | 20.996511 | 43.126229 | 17.476389 | 3.520122 |
| 6h | 21.009413 | 43.135182 | 18.350151 | 2.659262 |

The selected model (feature set A) beats the frozen baseline at every
horizon on TEST, with the margin narrowing as the horizon lengthens,
consistent with the validation-time pattern.

## Integrity checks

- Frozen configurations were recorded from the Phase 5C report before any
  TEST partition was read.
- Zero TEST rows were used for fitting or preprocessing
  (`test_rows_used_for_fitting_or_preprocessing: 0`); preprocessing and the
  model were fit on TRAIN+VALIDATION only.
- A and B TEST key populations are identical at every horizon
  (`A_B_key_symmetric_difference: 0`).
- Zero duplicate `(location_id, target_time_utc, horizon_hours)` keys and
  zero TRAIN+VALIDATION rows found inside the TEST period
  (`forbidden_train_validation_rows_in_test_period: 0`).
- Gold predictions contain 638,176 rows total (sum of A+B TEST rows across
  three horizons) with zero null/NaN predictions and unique
  `(location_id, target_time_utc, horizon_hours, feature_set)` keys.
- `pickup_count` (the target) does not appear in either feature contract.
- The identical zero-clipping nonnegative rule was applied to A and B at
  every horizon.
- No model, feature, or hyperparameter change occurred after TEST access
  (`model_selection_or_reselection_after_test: false`).
- Six focused Spark tests validate the frozen configuration values, the
  frozen baseline values, TEST-month partition coverage, per-horizon
  estimator hyperparameters, and reused feature contracts; all six passed.

The machine-readable report is written to
`s3a://bigdata/gold/metrics/final_test_evaluation_report.json` with a local
ignored copy at `data/silver/final_test_evaluation_report.json`. Compact
final predictions are written to `s3a://bigdata/gold/predictions`
partitioned by `horizon_hours` and `feature_set`.
