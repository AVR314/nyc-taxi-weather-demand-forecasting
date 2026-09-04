# Design Document — NYC Taxi Weather Demand Forecasting

## Problem and research question

Can weather information improve short-term NYC taxi demand forecasting, and
how does its predictive value change across 1-hour, 3-hour, and 6-hour
horizons? The target is hourly `pickup_count` per NYC Taxi Zone.

## Data sources

- **NYC TLC Yellow Taxi trip records**, calendar year 2025, official Parquet
  files from `https://d37ci6vzurychx.cloudfront.net/trip-data` (12 monthly
  files, 829,973,299 bytes) plus the official Taxi Zone lookup/geographic
  reference.
- **Open-Meteo ECMWF IFS Single Runs** forecast JSON
  (`single-runs-api.open-meteo.com`), five fixed NYC points, seven variables
  (`temperature_2m`, `relative_humidity_2m`, `precipitation`, `snowfall`,
  `weather_code`, `wind_speed_10m`, `wind_gusts_10m`), acquired with a
  conservative six-hour publication lag so only information available before
  each forecast target hour is used.

## Architecture and data flow

Bronze (raw, unmodified) → Spark ETL → Silver (cleaned, joined, leakage-safe
features) → chronological split/baselines → Spark ML (validation-only
selection) → frozen final evaluation → Gold (predictions, metrics). See
`docs/architecture_diagram.md` for the diagram.

- **MinIO** is the S3-compatible object store for all three layers
  (`bigdata` bucket), satisfying the assignment's Big Data storage
  requirement without extra infrastructure.
- **Apache Spark** (3.5.7, standalone, one master/one worker) performs every
  meaningful transformation: taxi cleaning/validation, Zone×Hour demand
  aggregation, weather join, feature engineering, chronological splitting,
  baseline scoring, model training, and frozen test evaluation.
- **Bronze** preserves raw TLC Parquet and raw weather JSON unchanged, with
  provenance (checksums, request parameters).
- **Silver** contains validated/cleaned taxi records, the complete Zone×Hour
  demand grid, weather records, the demand-weather join, and leakage-safe
  modeling features.
- **Gold** contains only final frozen TEST predictions and metrics.

## Time axis and DST policy

TLC Parquet timestamps are timezone-naive with no documented offset. The
project declares `America/New_York` as the explicit modeling convention for
calendar year 2025 (`[2025-01-01T05:00Z, 2026-01-01T05:00Z)` in UTC). The
2025 spring-forward nonexistent hour and fall-back ambiguous hour are
quarantined (excluded), never shifted or guessed. See
`docs/time_axis_audit.md`.

## Leakage-safe forecasting design

For target time `t` and horizon `h`, every predictor is computed strictly
from information at or before cutoff `t-h`; rolling windows end at the
cutoff. Weather predictors use only the forecast run available at or before
the cutoff under the six-hour publication lag, never observed/reanalysis
weather. Target demand never appears as a predictor.

## 74-zone modeling scope

Taxi Zones are not chosen arbitrarily: the smallest full-year,
demand-ranked zone set covering at least 95% of accepted five-borough
pickups was selected — 74 zones covering 46,215,963 of 48,601,811 pickups
(95.09%).

## Feature engineering

Feature set **A** (13 columns): Taxi Zone identity, calendar
(hour/day-of-week/month/weekend), demand at cutoff, previous-day/week
same-local-hour demand, trailing mean, and trailing 24h standard deviation.
Feature set **B** = A plus the seven approved weather variables and their
missingness/availability indicators. No imputation is performed anywhere;
missing values are preserved and rows are only included in the primary
paired population when both demand history and weather are complete.

## Chronological Train/Validation/Test protocol

Fixed, non-random, half-open `America/New_York` splits: train
`[2025-01-01, 2025-09-01)`, validation `[2025-09-01, 2025-11-01)`, test
`[2025-11-01, 2026-01-01)`. The same `paired_evaluation_eligible` population
is used for A and B at every horizon.

## Baseline strategy

Three non-ML baselines (persistence, previous-day seasonal naive,
previous-week seasonal naive) were scored on validation; the previous-week
baseline was selected by validation MAE alone (15.122268) before any ML
model was trained.

## ML model-selection protocol

Two model families (Regularized Linear Regression, Gradient-Boosted Trees)
with small predeclared grids were trained on train only and scored on
validation only, per horizon and feature set. Regularized Linear Regression
on feature set A was selected at every horizon (validation MAE 12.326950 /
14.180445 / 14.457571 for 1h/3h/6h), beating the baseline by 18.48% / 6.23%
/ 4.40%. Weather (B) did not reduce validation MAE for either family at any
horizon (−0.18% to −1.13%).

## Final frozen TEST protocol

The Phase 5C-selected model family and per-horizon/per-feature-set
hyperparameters were frozen before any TEST access. Preprocessing and the
model were refit on TRAIN+VALIDATION only and scored exactly once on the
frozen TEST partitions, with the identical zero-clipping nonnegative rule.
No model, feature, or hyperparameter choice changed afterward.

## Final measured results (TEST)

| Horizon | Feature set | MAE | RMSE |
|---|---|---:|---:|
| 1h | A | 13.789029 | 24.889088 |
| 1h | B | 13.775351 | 24.882623 |
| 3h | A | 17.476389 | 33.478442 |
| 3h | B | 17.474982 | 33.467007 |
| 6h | A | 18.350151 | 35.703538 |
| 6h | B | 18.369958 | 35.706288 |

The selected model (A) beats the frozen previous-week baseline test MAE
(20.969355 / 20.996511 / 21.009413) by 7.180326 / 3.520122 / 2.659262 at
every horizon.

## Weather A/B conclusion

Weather's measured TEST effect is small and inconsistent in sign: +0.099%
(1h), +0.008% (3h), −0.108% (6h) MAE change. Combined with the negative
validation-time deltas (−0.18% to −1.13%), weather adds no material,
reliable incremental predictive value over demand/calendar features alone
in this experiment, at any of the three horizons.

## Major limitations

- Forecast weather (not observed) is used for prediction realism, but the
  ECMWF single-run archive and six-hour lag are an approximation of true
  operational forecast availability.
- One calendar year limits exposure to inter-annual variability.
- 74 zones cover 95% of demand but exclude low-volume zones entirely.
- Quarantined DST hours are excluded rather than imputed, slightly reducing
  coverage near the two 2025 transition dates.
- Linear Regression was selected on measured validation MAE; it does not
  capture nonlinear spatial-temporal interactions that a tuned
  Gradient-Boosted Trees model might.

## Key engineering/research trade-offs

- **MinIO only** (no Elasticsearch/Kibana): satisfies the Big Data storage
  requirement with less operational complexity; adding a search/analytics
  layer was not justified by the core research question.
- **Chronological, non-random splits**: sacrifices statistical variance
  reduction for temporal validity, which is required for honest forecasting
  evaluation.
- **No imputation**: preserves evidence of real data gaps at the cost of a
  smaller paired-evaluation population.
- **Small predeclared hyperparameter grids**: avoids overfitting to
  validation through extensive tuning, at the cost of potentially leaving
  performance on the table.
