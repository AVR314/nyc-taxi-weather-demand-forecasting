# Chronological Splits and Baselines Validation

## Frozen protocol and populations

Target time is split by `America/New_York` wall time with half-open intervals:
train `[2025-01-01, 2025-09-01)`, validation `[2025-09-01, 2025-11-01)`, and
test `[2025-11-01, 2026-01-01)`. The corresponding endpoint instants are
`05:00Z/04:00Z`, `04:00Z/04:00Z`, and `04:00Z/05:00Z`. The full run found no
rows outside the protocol, no timestamp overlap, and strict measured ordering
between the three splits.

Both future weather and no-weather models use the same rows where
`paired_evaluation_eligible` is true. No missing values were imputed and the
full feature data were not duplicated.

| Split | Horizon | Candidates | Paired | Excluded | Demand-history incomplete | Weather missing |
|---|---:|---:|---:|---:|---:|---:|
| Train | 1h | 431,494 | 409,590 | 21,904 | 12,580 | 9,324 |
| Train | 3h | 431,494 | 409,590 | 21,904 | 12,580 | 9,324 |
| Train | 6h | 431,494 | 409,590 | 21,904 | 12,580 | 9,324 |
| Validation | 1h | 108,336 | 108,336 | 0 | 0 | 0 |
| Validation | 3h | 108,336 | 108,336 | 0 | 0 | 0 |
| Validation | 6h | 108,336 | 108,336 | 0 | 0 | 0 |
| Test | 1h | 108,262 | 106,412 | 1,850 | 1,850 | 0 |
| Test | 3h | 108,262 | 106,338 | 1,924 | 1,924 | 0 |
| Test | 6h | 108,262 | 106,338 | 1,924 | 1,924 | 0 |

There were no rows missing both demand history and weather. Thus each exclusion
reason in the table is also mutually exclusive for this run.

## Baseline metrics

Metrics use only the paired validation and frozen test populations. MAE is the
selection metric and RMSE is secondary.

| Split | Horizon | Baseline | Rows | MAE | RMSE |
|---|---:|---|---:|---:|---:|
| Validation | Overall | Persistence | 325,008 | 39.748108 | 75.011838 |
| Validation | Overall | Previous day | 325,008 | 21.330961 | 44.829499 |
| Validation | Overall | Previous week | 325,008 | 15.122268 | 29.725824 |
| Validation | 1h | Persistence | 108,336 | 18.293411 | 33.147680 |
| Validation | 3h | Persistence | 108,336 | 38.473444 | 69.244033 |
| Validation | 6h | Persistence | 108,336 | 62.477468 | 104.818046 |
| Validation | 1h/3h/6h | Previous day | 108,336 each | 21.330961 | 44.829499 |
| Validation | 1h/3h/6h | Previous week | 108,336 each | 15.122268 | 29.725824 |
| Test | Overall | Persistence | 319,088 | 38.076568 | 72.231622 |
| Test | Overall | Previous day | 319,088 | 20.686670 | 42.186395 |
| Test | Overall | Previous week | 319,088 | 20.991755 | 43.118488 |
| Test | 1h | Persistence | 106,412 | 17.498205 | 31.711622 |
| Test | 3h | Persistence | 106,338 | 36.687026 | 66.136757 |
| Test | 6h | Persistence | 106,338 | 60.058794 | 101.367918 |
| Test | 1h | Previous day | 106,412 | 20.676277 | 42.189496 |
| Test | 3h | Previous day | 106,338 | 20.676353 | 42.160683 |
| Test | 6h | Previous day | 106,338 | 20.707386 | 42.208991 |
| Test | 1h | Previous week | 106,412 | 20.969355 | 43.094060 |
| Test | 3h | Previous week | 106,338 | 20.996511 | 43.126229 |
| Test | 6h | Previous week | 106,338 | 21.009413 | 43.135182 |

Previous-week seasonal naive is selected solely because it has the lowest
overall validation MAE. Test metrics were computed only for the frozen report
and did not participate in selection.

Across 74 zones, validation median zone MAE was 28.442851 for persistence,
17.655055 for previous day, and 12.516393 for previous week. Test medians were
28.875928, 15.667208, and 16.847403 respectively. Full compact min, quartile,
mean, and max summaries are retained in the machine-readable Silver manifest.

## Integrity checks

The full run found zero duplicate feature or paired-population keys, zero
paired-definition mismatches, zero null baseline predictions or targets, and
zero source timestamps after prediction cutoff. Feature sets A and B reference
the same 1,872,866 keys. Target demand is an evaluation value only, never a
baseline predictor. Seven focused Spark tests validate the split boundaries,
population rules, metrics, source timing, key uniqueness, validation-only
selection, and allowed predictor definitions.
