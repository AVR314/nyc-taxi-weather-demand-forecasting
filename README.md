# NYC Taxi Weather Demand Forecasting

An end-to-end Big Data + ML pipeline that forecasts hourly NYC taxi demand
per Taxi Zone and measures whether weather forecasts add predictive value
at 1-hour, 3-hour, and 6-hour horizons.

## Research question

Can weather information improve short-term NYC taxi demand forecasting, and
how does its predictive value change across 1h, 3h, and 6h horizons?

## Key conclusion

A Regularized Linear Regression model on demand/calendar/zone features beat
the frozen previous-week seasonal-naive baseline at every horizon on the
frozen TEST set (MAE 13.789 / 17.476 / 18.350 vs baseline 20.969 / 20.997 /
21.009 for 1h/3h/6h). **Adding weather forecasts had no material,
consistent incremental predictive value**: measured validation deltas were
negative at every horizon (−0.18% to −1.13%), and TEST deltas were small and
changed sign by horizon (+0.099%, +0.008%, −0.108%). See
`docs/design_document.md` and `docs/final_test_evaluation_validation.md`
for full evidence.

## Final results (frozen TEST)

| Horizon | Feature set | Rows | MAE | RMSE | vs. baseline MAE |
|---|---|---:|---:|---:|---:|
| 1h | A (no weather) | 106,412 | 13.789029 | 24.889088 | −7.180326 |
| 1h | B (+ weather) | 106,412 | 13.775351 | 24.882623 | — |
| 3h | A (no weather) | 106,338 | 17.476389 | 33.478442 | −3.520122 |
| 3h | B (+ weather) | 106,338 | 17.474982 | 33.467007 | — |
| 6h | A (no weather) | 106,338 | 18.350151 | 35.703538 | −2.659262 |
| 6h | B (+ weather) | 106,338 | 18.369958 | 35.706288 | — |

Baseline: previous-week seasonal naive, frozen TEST MAE 20.969355 / 20.996511 / 21.009413 (1h/3h/6h).

## Architecture overview

```
NYC TLC Yellow Taxi Parquet + Open-Meteo ECMWF forecast JSON
  -> MinIO Bronze (raw, unmodified)
  -> Apache Spark ETL (clean, aggregate, join, engineer leakage-safe features)
  -> MinIO Silver (cleaned taxi, demand grid, weather, join, modeling features)
  -> chronological Train/Validation/frozen-Test split + non-ML baselines
  -> Spark ML validation-only model selection (Linear Regression vs GBT, A vs B)
  -> frozen final Linear Regression evaluation on Test (A and B)
  -> MinIO Gold (predictions + metrics)
```

Full diagram: `docs/architecture_diagram.md`. Design rationale and
trade-offs: `docs/design_document.md`.

## Technologies

- **Apache Spark** 3.5.7 (standalone, one master + one worker) — all
  meaningful ETL, feature engineering, and ML transformation.
- **MinIO** (S3-compatible object store) — Bronze/Silver/Gold layers; the
  required Big Data storage technology. Elasticsearch/Kibana were
  evaluated and explicitly excluded from the final architecture (see
  `DATA_DECISIONS.md`).
- **PySpark ML** (`LinearRegression`, `GBTRegressor`) for model training and
  selection.
- **Docker Compose** for reproducible, host-install-free infrastructure.
- **Python 3.12.8** for Bronze ingestion and feasibility tooling.

## Dataset sources

- NYC TLC Yellow Taxi trip records, calendar year 2025:
  `https://d37ci6vzurychx.cloudfront.net/trip-data` (official monthly
  Parquet files) plus the official Taxi Zone lookup/geographic reference.
- Open-Meteo ECMWF IFS Single Runs forecast JSON:
  `https://single-runs-api.open-meteo.com/v1/forecast` (five NYC points,
  seven weather variables, six-hour publication lag).

## Repository structure

```
src/
  bronze_ingestion/     Bronze raw-data acquisition (taxi + weather)
  silver_etl/            Spark cleaning, demand grid, weather join
  modeling_features/      Leakage-safe feature engineering
  forecast_baselines/     Chronological splits + non-ML baselines
  ml_selection/           Validation-only ML candidate selection
  final_evaluation/       Frozen final TEST evaluation + Gold outputs
tests/                    Focused unit/Spark tests per stage
docs/                     Validation reports, design document, diagram
scripts/                  Feasibility profiling and infrastructure smoke tests
data/                     Local ignored working copies of manifests/reports
compose.yaml, docker/     Infrastructure definition
AGENTS.md                 Project rules and engineering process
DATA_DECISIONS.md         Approved decisions, evidence, and trade-offs
PROJECT_STATUS.md         Phase-by-phase completion log
REQUIREMENTS_TRACEABILITY.md  Assignment requirement status
```

## Reproducible setup and run instructions

The sections below walk through every pipeline stage in order: infrastructure
start-up, Bronze ingestion, Silver ETL, feature engineering, baselines, ML
selection, and the frozen final evaluation. Each stage lists its focused test
command followed by its full-run command.

## Phase 2 infrastructure

The local infrastructure contains only MinIO, a one-shot MinIO initializer, one
Spark master, and one Spark worker. Java, Spark, Hadoop, and the S3A connector
remain inside containers.

Pinned components:

- Apache Spark 3.5.7, Scala 2.12, Java 17, and Hadoop 3.3.4
- `hadoop-aws` 3.3.4 and `aws-java-sdk-bundle` 1.12.262
- MinIO `RELEASE.2025-09-07T16-13-09Z`
- MinIO Client `RELEASE.2025-08-13T08-35-41Z`

### Configure

From PowerShell in the repository root:

```powershell
Copy-Item .env.example .env
```

Change the local MinIO credentials in `.env`. The file is ignored by Git. Then
validate the rendered Compose model:

```powershell
docker compose --env-file .env config --quiet
```

### Start and inspect

```powershell
docker compose --env-file .env up --detach --build
docker compose --env-file .env ps --all
```

The initializer creates the `bigdata` bucket and marker objects for these
planned prefixes:

- `bronze/taxi/`
- `bronze/weather/`
- `silver/taxi_clean/`
- `silver/weather_clean/`
- `silver/demand_weather/`
- `gold/predictions/`
- `gold/metrics/`

MinIO is available at `http://localhost:9000`, its console at
`http://localhost:9001`, and the Spark master and worker UIs at
`http://localhost:8080` and `http://localhost:8081` by default. Host ports can
be changed in `.env`.

### Validate the Spark-MinIO round trip

The smoke test builds and starts the stack, confirms service health, submits a
job to the standalone Spark cluster, writes a synthetic three-row DataFrame as
Parquet through `s3a://`, reads it back, checks schema/count/values, removes the
smoke object, and shuts the stack down:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\smoke_test_infrastructure.ps1
```

Run the same checks before and after a service restart:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\smoke_test_infrastructure.ps1 -ValidateRestart
```

Add `-KeepRunning` to either command when the services should remain running.

### Stop

```powershell
docker compose --env-file .env down --remove-orphans
```

## Phase 3 Bronze ingestion

Bronze ingestion preserves the 12 official 2025 Yellow Taxi Parquet files,
official Taxi Zone references, and Open-Meteo ECMWF IFS Single Run JSON
responses unchanged in MinIO. It uses five coordinates, seven approved weather
variables, and the six-hour publication lag. Missing forecast data are reported;
observed weather is never substituted and Bronze performs no imputation.

Build the pinned Python 3.12.8 ingestion image and run its tests:

```powershell
docker compose --env-file .env --profile ingestion build bronze-ingest
docker run --rm --entrypoint python nyc-taxi-weather-bronze-ingest:python3.12 -m unittest discover -s /app/tests -v
```

Run or resume ingestion. Existing checksum-verified objects are reused, and the
default 3.8-second weather request interval stays below the provider's weighted
hourly limit for a five-coordinate request.

```powershell
docker compose --env-file .env --profile ingestion run --rm bronze-ingest
```

Regenerate the detailed missing-coverage report from stored raw responses,
without source requests or reacquisition:

```powershell
docker compose --env-file .env --profile ingestion run --rm bronze-ingest --finalize-missing-coverage
```

The machine-readable manifest, summary, raw inventory, validation report, and
weather missing-coverage report are stored below `bronze/manifests/` in the
`bigdata` bucket. Local working copies under `data/ingestion/` are ignored by
Git. The completed inventory contains 1,476 raw source objects totaling
843,318,877 bytes: 12 taxi files, 1,454 successful weather responses, eight
preserved provider-unavailable responses, and two Taxi Zone references.

### Time axis

The modeling year is calendar year 2025 in `America/New_York`, treated as a
declared project convention because TLC's timezone-naive timestamps have no
documented timezone or offset. It maps to weather targets from
`2025-01-01T05:00:00Z` inclusive to `2026-01-01T05:00:00Z` exclusive. The
spring-forward nonexistent local hour and fall-back ambiguous local hour are
quarantined rather than shifted or assigned an arbitrary fold. See
`docs/time_axis_audit.md` for evidence and exact DST behavior.

If the time-axis policy changes, update only the weather plan and acquire only
missing boundary runs:

```powershell
docker compose --env-file .env --profile ingestion run --rm bronze-ingest --update-weather-plan
```

## Phase 4 Silver ETL

Run the focused synthetic Spark tests:

```powershell
docker compose --env-file .env run --rm --no-deps spark-master /opt/spark/bin/spark-submit --master "local[2]" /opt/project/tests/test_silver_etl.py
```

Start MinIO and the standalone Spark cluster, then run the full 2025 ETL:

```powershell
docker compose --env-file .env up --detach minio minio-init spark-master spark-worker
docker compose --env-file .env exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 2g --conf spark.cores.max=2 /opt/project/src/silver_etl/job.py
```

The job overwrites only its Silver output paths, never Bronze. It writes the
machine-readable validation report to
`s3a://bigdata/silver/manifests/silver_etl_report.json`; a local ignored copy is
written to `data/silver/silver_etl_report.json`. Measured results and schemas are
summarized in `docs/silver_etl_validation.md`.

## Phase 5A modeling features

Run the focused synthetic Spark tests before the full feature job:

```powershell
docker compose --env-file .env run --rm --no-deps spark-master /opt/spark/bin/spark-submit --master "local[2]" /opt/project/tests/test_modeling_features.py
```

With MinIO and the standalone Spark cluster running, build the leakage-safe
feature dataset from the validated Silver outputs:

```powershell
docker compose --env-file .env up --detach minio minio-init spark-master spark-worker
docker compose --env-file .env exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 2g --conf spark.cores.max=2 /opt/project/src/modeling_features/job.py
```

The job writes partitioned Parquet to
`s3a://bigdata/silver/modeling_features/records` and its machine-readable report
to `s3a://bigdata/silver/manifests/modeling_features_report.json`. It filters to
the approved 74 zones and available targets, preserves horizons separately,
does not impute missing history or weather, and exposes a paired-evaluation
eligibility flag. Measured results are in
`docs/modeling_features_validation.md`.

## Phase 5B chronological splits and baselines

Run the focused split/baseline tests:

```powershell
docker compose --env-file .env run --rm --no-deps spark-master /opt/spark/bin/spark-submit --master "local[2]" /opt/project/tests/test_forecast_baselines.py
```

Then use the existing Silver feature dataset to evaluate the three non-ML
baselines and persist the compact protocol manifest:

```powershell
docker compose --env-file .env up --detach minio minio-init spark-master spark-worker
docker compose --env-file .env exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 2g --conf spark.cores.max=2 /opt/project/src/forecast_baselines/job.py
```

The fixed `America/New_York` half-open splits are train `[2025-01-01,
2025-09-01)`, validation `[2025-09-01, 2025-11-01)`, and test `[2025-11-01,
2026-01-01)`. Both future feature sets use the single population where
`paired_evaluation_eligible` is true. No imputation, random splitting, feature
data duplication, or ML training occurs. The manifest is written to
`s3a://bigdata/silver/manifests/chronological_splits_baselines_report.json`;
measured results are in `docs/chronological_splits_baselines_validation.md`.

## Phase 5C ML candidate selection

Run the focused tests:

```powershell
docker compose --env-file .env run --rm --no-deps spark-master /opt/spark/bin/spark-submit --master "local[2]" /opt/project/tests/test_ml_selection.py
```

Then train and select ML candidates on train/validation only:

```powershell
docker compose --env-file .env up --detach minio minio-init spark-master spark-worker
docker compose --env-file .env exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 2g --conf spark.cores.max=2 /opt/project/src/ml_selection/job.py
```

Regularized Linear Regression and Gradient-Boosted Trees are trained and
scored separately per horizon (1h/3h/6h) on feature sets A (demand/calendar
plus Taxi Zone identity) and B (A plus approved weather). Preprocessing is
fit on train only; test partitions are never read. The manifest is written
to `s3a://bigdata/silver/manifests/ml_candidate_selection_report.json`;
measured results are in `docs/ml_selection_validation.md`.

## Phase 5D final frozen TEST evaluation and Gold outputs

Run the focused tests:

```powershell
docker compose --env-file .env run --rm --no-deps spark-master /opt/spark/bin/spark-submit --master "local[2]" /opt/project/tests/test_final_evaluation.py
```

Then refit the Phase 5C-selected Regularized Linear Regression configuration
on train+validation and evaluate once on the frozen test partitions:

```powershell
docker compose --env-file .env up --detach minio minio-init spark-master spark-worker
docker compose --env-file .env exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 2g --conf spark.cores.max=2 /opt/project/src/final_evaluation/job.py
```

Model family, hyperparameters, feature contracts, and preprocessing are
frozen from Phase 5C validation-only selection; nothing changes based on
TEST results. Gradient-Boosted Trees is not evaluated because it was not
selected. Preprocessing and the model are refit on train+validation only
(test rows are never used for fitting or preprocessing) and scored once on
test. Compact final predictions are written to
`s3a://bigdata/gold/predictions` (partitioned by `horizon_hours` and
`feature_set`); the metrics manifest is written to
`s3a://bigdata/gold/metrics/final_test_evaluation_report.json`. Measured
results are in `docs/final_test_evaluation_validation.md`.

## Validation and testing

Every stage has a focused test suite (`tests/test_*.py`, run via
`spark-submit --master "local[2]"` as shown above) plus a full-run
machine-readable validation report under `s3a://bigdata/silver/manifests/`
or `s3a://bigdata/gold/metrics/`, mirrored to a human-readable summary in
`docs/`. Validated evidence includes zero leakage violations, zero
duplicate/missing keys, identical A/B populations, exact expected row
counts, and — for the frozen TEST evaluation — proof that no TEST row was
used for fitting or preprocessing and that no model/feature/hyperparameter
choice changed after TEST access.

## Limitations

- Forecast weather (ECMWF single runs, six-hour lag) approximates but does
  not exactly reproduce true operational forecast availability.
- One calendar year (2025) limits exposure to inter-annual variability.
- The 74-zone scope covers 95% of demand but excludes low-volume zones.
- 2025 DST transition hours are quarantined (excluded), not imputed.
- Only Regularized Linear Regression was carried to the frozen TEST
  evaluation; Gradient-Boosted Trees was evaluated at validation time only
  and was not selected.

## Open items

- Presentation slides, demo, and Q&A rehearsal are not yet prepared.
- University approval for solo (non-team-of-three) work is not yet
  confirmed; see `PROJECT_STATUS.md`.
