# NYC Taxi Weather Demand Forecasting

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
Git. The completed inventory contains 1,475 raw source objects totaling
843,310,328 bytes: 12 taxi files, 1,453 successful weather responses, eight
preserved provider-unavailable responses, and two Taxi Zone references.
