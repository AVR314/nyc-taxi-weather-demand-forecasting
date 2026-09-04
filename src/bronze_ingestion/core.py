"""Planning, validation, transfer, and manifest logic for Bronze ingestion."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import tempfile
import time
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import boto3
import pyarrow.parquet as pq
from botocore.client import Config
from botocore.exceptions import ClientError


BUCKET = "bigdata"
MANIFEST_KEY = "bronze/manifests/bronze_ingestion_manifest.json"
SUMMARY_KEY = "bronze/manifests/bronze_ingestion_summary.json"
INVENTORY_KEY = "bronze/manifests/raw_object_inventory.json"
VALIDATION_KEY = "bronze/manifests/bronze_validation_report.json"
MISSING_COVERAGE_KEY = "bronze/manifests/weather_missing_coverage.json"

TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_MONTHS = tuple(f"2025-{month:02d}" for month in range(1, 13))
TAXI_FILE_PATTERN = re.compile(r"^yellow_tripdata_(\d{4}-\d{2})\.parquet$")

REFERENCE_SOURCES = (
    {
        "name": "taxi_zone_lookup",
        "url": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
        "file_name": "taxi_zone_lookup.csv",
        "object_key": "bronze/reference/taxi_zone_lookup.csv",
        "format": "csv",
    },
    {
        "name": "taxi_zones_geographic_archive",
        "url": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip",
        "file_name": "taxi_zones.zip",
        "object_key": "bronze/reference/taxi_zones.zip",
        "format": "zip",
    },
)

WEATHER_API_URL = "https://single-runs-api.open-meteo.com/v1/forecast"
WEATHER_MODEL = "ecmwf_ifs"
WEATHER_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "snowfall",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
)
NYC_POINTS = (
    {"name": "Lower Manhattan", "slug": "lower_manhattan", "latitude": 40.7128, "longitude": -74.0060},
    {"name": "Bronx", "slug": "bronx", "latitude": 40.8448, "longitude": -73.8648},
    {"name": "Brooklyn", "slug": "brooklyn", "latitude": 40.6782, "longitude": -73.9442},
    {"name": "JFK / Queens", "slug": "jfk_queens", "latitude": 40.6413, "longitude": -73.7781},
    {"name": "Staten Island", "slug": "staten_island", "latitude": 40.5795, "longitude": -74.1502},
)
FORECAST_HORIZONS_HOURS = (1, 3, 6)
PUBLICATION_LAG_HOURS = 6
WEATHER_PLAN_VERSION = 2
NYC_TIMEZONE_NAME = "America/New_York"
NYC_TIMEZONE = ZoneInfo(NYC_TIMEZONE_NAME)
LOCAL_MODELING_YEAR_START = datetime(2025, 1, 1)
LOCAL_MODELING_YEAR_END_EXCLUSIVE = datetime(2026, 1, 1)
TARGET_START = LOCAL_MODELING_YEAR_START.replace(tzinfo=NYC_TIMEZONE).astimezone(UTC)
TARGET_END_EXCLUSIVE = LOCAL_MODELING_YEAR_END_EXCLUSIVE.replace(
    tzinfo=NYC_TIMEZONE
).astimezone(UTC)
TARGET_END = TARGET_END_EXCLUSIVE - timedelta(hours=1)


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_stream(stream: BinaryIO) -> str:
    digest = hashlib.sha256()
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def local_wall_time_candidates_utc(value: datetime) -> tuple[datetime, ...]:
    """Return the real UTC instants represented by a naive NYC wall time."""
    if value.tzinfo is not None:
        raise ValueError("TLC wall-time conversion requires a timezone-naive datetime")
    candidates: set[datetime] = set()
    for fold in (0, 1):
        candidate = value.replace(tzinfo=NYC_TIMEZONE, fold=fold).astimezone(UTC)
        round_trip = candidate.astimezone(NYC_TIMEZONE)
        if round_trip.replace(tzinfo=None) == value and round_trip.fold == fold:
            candidates.add(candidate)
    return tuple(sorted(candidates))


def taxi_local_wall_time_to_utc(value: datetime) -> datetime:
    """Convert an unambiguous TLC local-wall timestamp under the project policy."""
    candidates = local_wall_time_candidates_utc(value)
    if not candidates:
        raise ValueError(f"nonexistent America/New_York wall time: {value.isoformat()}")
    if len(candidates) > 1:
        raise ValueError(f"ambiguous America/New_York wall time: {value.isoformat()}")
    return candidates[0]


def floor_to_ecmwf_cycle(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("ECMWF cycle planning requires timezone-aware datetimes")
    return value.replace(hour=(value.hour // 6) * 6, minute=0, second=0, microsecond=0)


def required_weather_plan() -> dict[datetime, list[dict[str, Any]]]:
    """Map each leakage-safe eligible run to its required targets and horizons."""
    plan: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
    target = TARGET_START
    while target <= TARGET_END:
        for horizon in FORECAST_HORIZONS_HOURS:
            prediction_cutoff = target - timedelta(hours=horizon)
            latest_eligible = floor_to_ecmwf_cycle(
                prediction_cutoff - timedelta(hours=PUBLICATION_LAG_HOURS)
            )
            plan[latest_eligible].append(
                {
                    "target_time": target,
                    "horizon_hours": horizon,
                    "prediction_cutoff": prediction_cutoff,
                }
            )
        target += timedelta(hours=1)
    return dict(sorted(plan.items()))


def weather_request(run: datetime) -> tuple[str, dict[str, str]]:
    parameters = {
        "latitude": ",".join(str(point["latitude"]) for point in NYC_POINTS),
        "longitude": ",".join(str(point["longitude"]) for point in NYC_POINTS),
        "hourly": ",".join(WEATHER_VARIABLES),
        "models": WEATHER_MODEL,
        "run": run.strftime("%Y-%m-%dT%H:%M"),
        "forecast_hours": "24",
        "timezone": "UTC",
    }
    return f"{WEATHER_API_URL}?{urlencode(parameters)}", parameters


def weather_object_key(run: datetime) -> str:
    return f"bronze/weather/{WEATHER_MODEL}/run={run:%Y-%m-%dT%H-%M}/response.json"


@dataclass
class HttpRecorder:
    logical_requests: int = 0
    attempts: int = 0
    retries: int = 0
    failures: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "logical_requests": self.logical_requests,
            "attempts": self.attempts,
            "retries": self.retries,
            "final_failure_count": len(self.failures),
            "final_failures": self.failures,
        }


class HttpClient:
    def __init__(self, *, timeout_seconds: int = 180, max_attempts: int = 5) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.stats = HttpRecorder()

    def open(self, url: str, *, method: str = "GET"):
        self.stats.logical_requests += 1
        last_error: Exception | None = None
        last_body = b""
        last_status: int | None = None
        attempts_made = 0
        for attempt in range(1, self.max_attempts + 1):
            attempts_made = attempt
            self.stats.attempts += 1
            request = Request(
                url,
                method=method,
                headers={"User-Agent": "nyc-taxi-weather-bronze-ingestion/1.0"},
            )
            try:
                response = urlopen(request, timeout=self.timeout_seconds)
                if response.status != 200:
                    response.close()
                    raise RuntimeError(f"unexpected HTTP status {response.status}")
                return response, attempt
            except (HTTPError, URLError, TimeoutError, RuntimeError) as exc:
                last_error = exc
                if isinstance(exc, HTTPError):
                    last_status = exc.code
                    last_body = exc.read()
                    exc.close()
                retryable = not isinstance(exc, HTTPError) or exc.code == 429 or exc.code >= 500
                if attempt >= self.max_attempts or not retryable:
                    break
                self.stats.retries += 1
                retry_after = 0.0
                if isinstance(exc, HTTPError):
                    try:
                        retry_after = float(exc.headers.get("Retry-After", "0"))
                    except ValueError:
                        retry_after = 0.0
                time.sleep(max(retry_after, min(2 ** (attempt - 1), 30)))
        self.stats.failures.append(
            {
                "url": url,
                "method": method,
                "status": last_status,
                "response_body": last_body.decode("utf-8", errors="replace"),
                "attempts": attempts_made,
                "error": repr(last_error),
                "at": utc_now(),
            }
        )
        raise HttpRequestError(
            f"HTTP {method} failed after {attempts_made} attempt(s): {url}",
            status=last_status,
            body=last_body,
            attempts=attempts_made,
        ) from last_error

    def head(self, url: str) -> tuple[dict[str, str], int]:
        response, attempts = self.open(url, method="HEAD")
        try:
            return {key.lower(): value for key, value in response.headers.items()}, attempts
        finally:
            response.close()

    def get_bytes(self, url: str) -> tuple[bytes, dict[str, str], int]:
        response, attempts = self.open(url)
        try:
            content = response.read()
            headers = {key.lower(): value for key, value in response.headers.items()}
        finally:
            response.close()
        if not content:
            raise ValueError(f"empty HTTP response: {url}")
        expected = headers.get("content-length")
        if expected is not None and int(expected) != len(content):
            raise ValueError(f"HTTP content-length mismatch for {url}")
        return content, headers, attempts

    def download(self, url: str, destination: Path) -> tuple[int, str, dict[str, str], int]:
        response, attempts = self.open(url)
        digest = hashlib.sha256()
        byte_count = 0
        try:
            headers = {key.lower(): value for key, value in response.headers.items()}
            with destination.open("wb") as output:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
                    digest.update(chunk)
                    byte_count += len(chunk)
        finally:
            response.close()
        if byte_count <= 0:
            raise ValueError(f"empty HTTP response: {url}")
        expected = headers.get("content-length")
        if expected is not None and int(expected) != byte_count:
            raise ValueError(f"HTTP content-length mismatch for {url}")
        return byte_count, digest.hexdigest(), headers, attempts


class ObjectStore:
    def __init__(self) -> None:
        endpoint = os.environ.get("S3_ENDPOINT_URL", "http://minio:9000")
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def head(self, key: str) -> dict[str, Any] | None:
        try:
            return self.client.head_object(Bucket=BUCKET, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    def get_bytes(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=BUCKET, Key=key)
        return response["Body"].read()

    def get_json(self, key: str) -> dict[str, Any] | None:
        try:
            return json.loads(self.get_bytes(key))
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
                return None
            raise

    def put_bytes(self, key: str, content: bytes, *, content_type: str, metadata: dict[str, str]) -> None:
        self.client.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=content,
            ContentLength=len(content),
            ContentType=content_type,
            Metadata=metadata,
        )

    def upload_file(self, key: str, source: Path, *, content_type: str, metadata: dict[str, str]) -> None:
        self.client.upload_file(
            str(source),
            BUCKET,
            key,
            ExtraArgs={"ContentType": content_type, "Metadata": metadata},
        )

    def put_json(self, key: str, value: Any) -> bytes:
        content = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
        self.put_bytes(
            key,
            content,
            content_type="application/json",
            metadata={"sha256": sha256_bytes(content), "generated-at": utc_now()},
        )
        return content

    def sha256(self, key: str) -> str:
        response = self.client.get_object(Bucket=BUCKET, Key=key)
        return sha256_stream(response["Body"])

    def list(self, prefix: str) -> list[dict[str, Any]]:
        paginator = self.client.get_paginator("list_objects_v2")
        results: list[dict[str, Any]] = []
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for item in page.get("Contents", []):
                results.append(
                    {
                        "object_key": item["Key"],
                        "bytes": item["Size"],
                        "etag": item["ETag"].strip('"'),
                        "last_modified": item["LastModified"].astimezone(UTC).isoformat().replace("+00:00", "Z"),
                    }
                )
        return sorted(results, key=lambda item: item["object_key"])


class HttpRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status: int | None,
        body: bytes,
        attempts: int,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.body = body
        self.attempts = attempts


def reusable_entry(
    store: ObjectStore,
    previous: dict[str, Any] | None,
    *,
    object_key: str,
    expected_source_bytes: int | None = None,
) -> dict[str, Any] | None:
    if previous is None:
        return None
    head = store.head(object_key)
    if head is None or head.get("ContentLength", 0) <= 0:
        return None
    expected_bytes = previous.get("bytes")
    expected_sha = previous.get("sha256")
    if expected_bytes != head["ContentLength"] or not expected_sha:
        return None
    if expected_source_bytes is not None and expected_source_bytes != head["ContentLength"]:
        return None
    if head.get("Metadata", {}).get("sha256") != expected_sha:
        return None
    result = dict(previous)
    result["reused_existing_object"] = True
    result["validated_at"] = utc_now()
    return result


def verify_uploaded_object(store: ObjectStore, key: str, expected_bytes: int, expected_sha: str) -> None:
    head = store.head(key)
    if head is None or head["ContentLength"] != expected_bytes:
        raise ValueError(f"uploaded object size mismatch: s3a://{BUCKET}/{key}")
    if head.get("Metadata", {}).get("sha256") != expected_sha:
        raise ValueError(f"uploaded object checksum metadata mismatch: s3a://{BUCKET}/{key}")
    actual_sha = store.sha256(key)
    if actual_sha != expected_sha:
        raise ValueError(f"uploaded object content checksum mismatch: s3a://{BUCKET}/{key}")


def parquet_metadata(path: Path, expected_month: str) -> dict[str, Any]:
    match = TAXI_FILE_PATTERN.match(path.name)
    if match is None or match.group(1) != expected_month:
        raise ValueError(f"taxi file name does not match expected month {expected_month}: {path.name}")
    with path.open("rb") as stream:
        if stream.read(4) != b"PAR1":
            raise ValueError(f"missing Parquet leading magic bytes: {path.name}")
        stream.seek(-4, io.SEEK_END)
        if stream.read(4) != b"PAR1":
            raise ValueError(f"missing Parquet trailing magic bytes: {path.name}")
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows <= 0 or parquet.metadata.num_row_groups <= 0:
        raise ValueError(f"empty Parquet metadata: {path.name}")
    next(parquet.iter_batches(batch_size=1))
    schema_names = parquet.schema_arrow.names
    required = {"tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID"}
    missing = sorted(required.difference(schema_names))
    if missing:
        raise ValueError(f"missing required taxi fields in {path.name}: {missing}")

    pickup_index = schema_names.index("tpep_pickup_datetime")
    minima: list[datetime] = []
    maxima: list[datetime] = []
    for row_group in range(parquet.metadata.num_row_groups):
        stats = parquet.metadata.row_group(row_group).column(pickup_index).statistics
        if stats is not None and stats.has_min_max:
            minima.append(stats.min)
            maxima.append(stats.max)
    if not minima or not maxima:
        raise ValueError(f"pickup timestamp statistics unavailable in {path.name}")
    month_start = datetime.strptime(expected_month, "%Y-%m")
    month_end = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    if max(maxima) < month_start or min(minima) >= month_end:
        raise ValueError(f"pickup timestamp coverage does not overlap {expected_month}: {path.name}")
    return {
        "row_count": parquet.metadata.num_rows,
        "row_groups": parquet.metadata.num_row_groups,
        "schema_fields": schema_names,
        "pickup_statistics_min": min(minima).isoformat(),
        "pickup_statistics_max": max(maxima).isoformat(),
        "expected_month_overlap": True,
    }


def validate_reference(path: Path, source_format: str) -> dict[str, Any]:
    if path.stat().st_size <= 0:
        raise ValueError(f"empty reference artifact: {path.name}")
    if source_format == "csv":
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            rows = list(reader)
        required = {"LocationID", "Borough", "Zone", "service_zone"}
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError("Taxi Zone lookup is missing required columns")
        return {"row_count": len(rows), "columns": reader.fieldnames}
    if source_format == "zip":
        with zipfile.ZipFile(path) as archive:
            bad_member = archive.testzip()
            names = archive.namelist()
        if bad_member is not None:
            raise ValueError(f"corrupt geographic archive member: {bad_member}")
        required_suffixes = {".shp", ".shx", ".dbf", ".prj"}
        present_suffixes = {Path(name).suffix.lower() for name in names}
        if not required_suffixes.issubset(present_suffixes):
            raise ValueError("Taxi Zone geographic archive lacks required shapefile members")
        return {"members": names, "member_count": len(names)}
    raise ValueError(f"unsupported reference format: {source_format}")


def summarize_weather_payload(
    content: bytes,
    run: datetime,
    required_targets: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    payload = json.loads(content)
    if not isinstance(payload, list) or len(payload) != len(NYC_POINTS):
        raise ValueError(f"expected {len(NYC_POINTS)} weather locations for run {run.isoformat()}")

    response_missing = Counter()
    predictor_missing = Counter()
    missing_variable_names: set[str] = set()
    missing_predictor_slots: list[dict[str, Any]] = []
    provider_points: list[dict[str, Any]] = []
    time_indexes: list[dict[str, int]] = []

    for requested, location in zip(NYC_POINTS, payload, strict=True):
        if not isinstance(location, dict) or "hourly" not in location:
            raise ValueError(f"invalid Open-Meteo location payload for run {run.isoformat()}")
        hourly = location["hourly"]
        times = hourly.get("time")
        if not isinstance(times, list) or not times:
            raise ValueError(f"missing hourly timestamps for run {run.isoformat()}")
        time_indexes.append({value: index for index, value in enumerate(times)})
        for variable in WEATHER_VARIABLES:
            values = hourly.get(variable)
            if not isinstance(values, list):
                missing_variable_names.add(variable)
                response_missing[variable] += len(times)
                continue
            if len(values) != len(times):
                raise ValueError(f"unaligned {variable} values for run {run.isoformat()}")
            response_missing[variable] += sum(value is None for value in values)
        provider_points.append(
            {
                "name": requested["name"],
                "slug": requested["slug"],
                "requested_latitude": requested["latitude"],
                "requested_longitude": requested["longitude"],
                "returned_latitude": location.get("latitude"),
                "returned_longitude": location.get("longitude"),
                "elevation": location.get("elevation"),
                "timezone": location.get("timezone"),
                "utc_offset_seconds": location.get("utc_offset_seconds"),
                "hour_count": len(times),
                "first_hour": times[0],
                "last_hour": times[-1],
            }
        )

    required_checks = 0
    missing_required_target_hours: set[str] = set()
    for required in required_targets:
        target_key = required["target_time"].strftime("%Y-%m-%dT%H:%M")
        for point_index, location in enumerate(payload):
            index = time_indexes[point_index].get(target_key)
            for variable in WEATHER_VARIABLES:
                required_checks += 1
                values = location["hourly"].get(variable)
                if index is None or not isinstance(values, list) or values[index] is None:
                    predictor_missing[variable] += 1
                    missing_predictor_slots.append(
                        {
                            "run_initialization_utc": run.isoformat(),
                            "target_time_utc": required["target_time"].isoformat(),
                            "prediction_cutoff_utc": required["prediction_cutoff"].isoformat(),
                            "horizon_hours": required["horizon_hours"],
                            "point": NYC_POINTS[point_index]["slug"],
                            "variable": variable,
                            "reason": "missing_or_null_in_provider_response",
                        }
                    )
                    missing_required_target_hours.add(
                        f"{target_key}|h={required['horizon_hours']}|point={NYC_POINTS[point_index]['slug']}"
                    )

    return {
        "provider_returned_points": provider_points,
        "response_missing_values_by_variable": dict(sorted(response_missing.items())),
        "missing_variable_names": sorted(missing_variable_names),
        "required_predictor_checks": required_checks,
        "required_predictor_missing_by_variable": dict(sorted(predictor_missing.items())),
        "missing_required_predictor_slots": missing_predictor_slots,
        "missing_required_target_point_horizon_count": len(missing_required_target_hours),
        "missing_required_target_point_horizon_examples": sorted(missing_required_target_hours)[:20],
    }


def unavailable_weather_coverage(
    run: datetime, required_targets: Iterable[dict[str, Any]]
) -> dict[str, Any]:
    required_targets = list(required_targets)
    slots = [
        {
            "run_initialization_utc": run.isoformat(),
            "target_time_utc": required["target_time"].isoformat(),
            "prediction_cutoff_utc": required["prediction_cutoff"].isoformat(),
            "horizon_hours": required["horizon_hours"],
            "point": point["slug"],
            "variable": variable,
            "reason": "provider_run_unavailable",
        }
        for required in required_targets
        for point in NYC_POINTS
        for variable in WEATHER_VARIABLES
    ]
    missing_target_point_horizons = len(required_targets) * len(NYC_POINTS)
    missing_per_variable = len(required_targets) * len(NYC_POINTS)
    return {
        "required_predictor_missing_by_variable": {
            variable: missing_per_variable for variable in WEATHER_VARIABLES
        },
        "missing_required_predictor_slots": slots,
        "missing_required_target_point_horizon_count": missing_target_point_horizons,
    }


def new_manifest() -> dict[str, Any]:
    return {
        "manifest_version": 1,
        "study_period": {
            "modeling_calendar_year": 2025,
            "local_timezone": NYC_TIMEZONE_NAME,
            "local_start_inclusive": LOCAL_MODELING_YEAR_START.isoformat(),
            "local_end_exclusive": LOCAL_MODELING_YEAR_END_EXCLUSIVE.isoformat(),
            "weather_target_start_utc_inclusive": TARGET_START.isoformat(),
            "weather_target_end_utc_inclusive": TARGET_END.isoformat(),
        },
        "generated_at": utc_now(),
        "taxi": [],
        "weather": [],
        "weather_failures": [],
        "reference": [],
        "design": {
            "weather_provider": "Open-Meteo",
            "weather_model": WEATHER_MODEL,
            "weather_source_nature": "historical forecast single runs; ex-ante predictors",
            "observed_or_reanalysis_predictors": False,
            "publication_lag_hours": PUBLICATION_LAG_HOURS,
            "weather_plan_version": WEATHER_PLAN_VERSION,
            "eligibility_rule": "run_initialization_utc + 6h <= prediction_cutoff_utc",
            "forecast_horizons_hours": list(FORECAST_HORIZONS_HOURS),
            "weather_variables": list(WEATHER_VARIABLES),
            "requested_points": list(NYC_POINTS),
            "timezone": "UTC",
            "taxi_timestamp_policy": (
                "treat timezone-naive TLC timestamps as America/New_York wall time for modeling; "
                "reject nonexistent and quarantine ambiguous wall times rather than guessing a fold"
            ),
        },
    }


def entry_index(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        entry["object_key"]: entry
        for section in ("taxi", "weather", "weather_failures", "reference")
        for entry in manifest.get(section, [])
    }


def checkpoint(store: ObjectStore, manifest: dict[str, Any], output_dir: Path) -> None:
    manifest["generated_at"] = utc_now()
    content = store.put_json(MANIFEST_KEY, manifest)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "bronze_ingestion_manifest.json").write_bytes(content)


def ingest_taxi(
    store: ObjectStore,
    http: HttpClient,
    previous: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for month in TAXI_MONTHS:
        file_name = f"yellow_tripdata_{month}.parquet"
        url = f"{TAXI_BASE_URL}/{file_name}"
        object_key = f"bronze/taxi/{file_name}"
        headers, head_attempts = http.head(url)
        source_bytes = int(headers.get("content-length", "0"))
        if source_bytes <= 0:
            raise ValueError(f"official taxi source is empty or lacks content length: {url}")
        reused = reusable_entry(
            store,
            previous.get(object_key),
            object_key=object_key,
            expected_source_bytes=source_bytes,
        )
        if reused is not None:
            reused["source_head_attempts_last_validation"] = head_attempts
            results.append(reused)
            print(f"TAXI_REUSED month={month} bytes={source_bytes}", flush=True)
            continue

        with tempfile.TemporaryDirectory(prefix="bronze-taxi-") as temp_dir:
            path = Path(temp_dir) / file_name
            byte_count, digest, response_headers, download_attempts = http.download(url, path)
            if byte_count != source_bytes:
                raise ValueError(f"taxi HEAD/GET size mismatch for {url}")
            parquet = parquet_metadata(path, month)
            ingested_at = utc_now()
            store.upload_file(
                object_key,
                path,
                content_type="application/vnd.apache.parquet",
                metadata={"sha256": digest, "month": month, "ingested-at": ingested_at},
            )
            verify_uploaded_object(store, object_key, byte_count, digest)
        result = {
            "artifact_type": "taxi_parquet",
            "month": month,
            "source_url": url,
            "file_name": file_name,
            "object_key": object_key,
            "s3a_uri": f"s3a://{BUCKET}/{object_key}",
            "bytes": byte_count,
            "sha256": digest,
            "ingested_at": ingested_at,
            "http_status": 200,
            "source_head_attempts": head_attempts,
            "source_download_attempts": download_attempts,
            "content_type": response_headers.get("content-type"),
            "parquet": parquet,
            "raw_object_sha256_verified": True,
            "reused_existing_object": False,
        }
        results.append(result)
        print(f"TAXI_INGESTED month={month} bytes={byte_count} sha256={digest}", flush=True)
    return results


def ingest_references(
    store: ObjectStore,
    http: HttpClient,
    previous: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for source in REFERENCE_SOURCES:
        headers, head_attempts = http.head(source["url"])
        source_bytes = int(headers.get("content-length", "0"))
        reused = reusable_entry(
            store,
            previous.get(source["object_key"]),
            object_key=source["object_key"],
            expected_source_bytes=source_bytes if source_bytes > 0 else None,
        )
        if reused is not None:
            results.append(reused)
            print(f"REFERENCE_REUSED name={source['name']} bytes={reused['bytes']}", flush=True)
            continue
        with tempfile.TemporaryDirectory(prefix="bronze-reference-") as temp_dir:
            path = Path(temp_dir) / source["file_name"]
            byte_count, digest, response_headers, download_attempts = http.download(source["url"], path)
            validation = validate_reference(path, source["format"])
            ingested_at = utc_now()
            content_type = "text/csv" if source["format"] == "csv" else "application/zip"
            store.upload_file(
                source["object_key"],
                path,
                content_type=content_type,
                metadata={"sha256": digest, "ingested-at": ingested_at},
            )
            verify_uploaded_object(store, source["object_key"], byte_count, digest)
        result = {
            "artifact_type": "taxi_zone_reference",
            "name": source["name"],
            "source_url": source["url"],
            "file_name": source["file_name"],
            "format": source["format"],
            "object_key": source["object_key"],
            "s3a_uri": f"s3a://{BUCKET}/{source['object_key']}",
            "bytes": byte_count,
            "sha256": digest,
            "ingested_at": ingested_at,
            "http_status": 200,
            "source_head_attempts": head_attempts,
            "source_download_attempts": download_attempts,
            "content_type": response_headers.get("content-type"),
            "validation": validation,
            "raw_object_sha256_verified": True,
            "reused_existing_object": False,
        }
        results.append(result)
        print(f"REFERENCE_INGESTED name={source['name']} bytes={byte_count} sha256={digest}", flush=True)
    return results


def ingest_weather(
    store: ObjectStore,
    http: HttpClient,
    previous: dict[str, dict[str, Any]],
    *,
    request_delay_seconds: float,
    checkpoint_callback,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    plan = required_weather_plan()
    results: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []
    total = len(plan)
    for index, (run, required_targets) in enumerate(plan.items(), start=1):
        object_key = weather_object_key(run)
        error_key = object_key.replace("response.json", "unavailable.json")
        reused = reusable_entry(store, previous.get(object_key), object_key=object_key)
        if reused is not None:
            if reused.get("weather_plan_version") != WEATHER_PLAN_VERSION:
                stored_content = store.get_bytes(object_key)
                if sha256_bytes(stored_content) != reused["sha256"]:
                    raise ValueError(f"stored weather checksum mismatch: {object_key}")
                reused.update(summarize_weather_payload(stored_content, run, required_targets))
            reused["weather_plan_version"] = WEATHER_PLAN_VERSION
            reused["required_target_horizon_pairs"] = len(required_targets)
            results.append(reused)
        elif (
            reused_failure := reusable_entry(
                store,
                previous.get(error_key),
                object_key=error_key,
            )
        ) is not None:
            if reused_failure.get("weather_plan_version") != WEATHER_PLAN_VERSION:
                reused_failure.update(unavailable_weather_coverage(run, required_targets))
            reused_failure["weather_plan_version"] = WEATHER_PLAN_VERSION
            reused_failure["required_target_horizon_pairs"] = len(required_targets)
            unavailable.append(reused_failure)
        else:
            if request_delay_seconds > 0:
                time.sleep(request_delay_seconds)
            url, parameters = weather_request(run)
            try:
                content, headers, attempts = http.get_bytes(url)
            except HttpRequestError as exc:
                try:
                    error_payload = json.loads(exc.body)
                    reason = str(error_payload.get("reason", ""))
                except (json.JSONDecodeError, AttributeError):
                    error_payload = None
                    reason = ""
                if exc.status != 400 or "requested model run is not available" not in reason.lower():
                    raise
                digest = sha256_bytes(exc.body)
                retrieved_at = utc_now()
                store.put_bytes(
                    error_key,
                    exc.body,
                    content_type="application/json",
                    metadata={
                        "sha256": digest,
                        "model": WEATHER_MODEL,
                        "run-init": run.strftime("%Y%m%dT%H%MZ"),
                        "retrieved-at": retrieved_at,
                    },
                )
                verify_uploaded_object(store, error_key, len(exc.body), digest)
                unavailable.append(
                    {
                        "artifact_type": "weather_unavailable_run_json",
                        "provider": "Open-Meteo",
                        "model": WEATHER_MODEL,
                        "run_initialization_utc": run.isoformat(),
                        "request_url": url,
                        "request_parameters": parameters,
                        "requested_coordinates": list(NYC_POINTS),
                        "timezone": "UTC",
                        "object_key": error_key,
                        "s3a_uri": f"s3a://{BUCKET}/{error_key}",
                        "bytes": len(exc.body),
                        "sha256": digest,
                        "retrieved_at": retrieved_at,
                        "http_status": exc.status,
                        "http_attempts": exc.attempts,
                        "provider_error": error_payload,
                        "weather_plan_version": WEATHER_PLAN_VERSION,
                        "required_target_horizon_pairs": len(required_targets),
                        **unavailable_weather_coverage(run, required_targets),
                        "raw_object_sha256_verified": True,
                        "reused_existing_object": False,
                    }
                )
                print(
                    f"WEATHER_UNAVAILABLE run={run.isoformat()} "
                    f"required_pairs={len(required_targets)} reason={reason}",
                    flush=True,
                )
                content = None
            if content is None:
                if index % 25 == 0 or index == total:
                    checkpoint_callback(results, unavailable)
                continue
            weather_validation = summarize_weather_payload(content, run, required_targets)
            digest = sha256_bytes(content)
            ingested_at = utc_now()
            store.put_bytes(
                object_key,
                content,
                content_type="application/json",
                metadata={
                    "sha256": digest,
                    "model": WEATHER_MODEL,
                    "run-init": run.strftime("%Y%m%dT%H%MZ"),
                    "ingested-at": ingested_at,
                },
            )
            verify_uploaded_object(store, object_key, len(content), digest)
            results.append(
                {
                    "artifact_type": "weather_single_run_json",
                    "provider": "Open-Meteo",
                    "model": WEATHER_MODEL,
                    "run_initialization_utc": run.isoformat(),
                    "available_after_utc": (run + timedelta(hours=PUBLICATION_LAG_HOURS)).isoformat(),
                    "publication_lag_hours": PUBLICATION_LAG_HOURS,
                    "request_url": url,
                    "request_parameters": parameters,
                    "requested_coordinates": list(NYC_POINTS),
                    "timezone": "UTC",
                    "object_key": object_key,
                    "s3a_uri": f"s3a://{BUCKET}/{object_key}",
                    "bytes": len(content),
                    "sha256": digest,
                    "retrieved_at": ingested_at,
                    "http_status": 200,
                    "http_attempts": attempts,
                    "content_type": headers.get("content-type"),
                    "weather_plan_version": WEATHER_PLAN_VERSION,
                    "required_target_horizon_pairs": len(required_targets),
                    **weather_validation,
                    "raw_object_sha256_verified": True,
                    "reused_existing_object": False,
                }
            )
        if index % 25 == 0 or index == total:
            checkpoint_callback(results, unavailable)
            reused_count = sum(
                item.get("reused_existing_object", False)
                for item in [*results, *unavailable]
            )
            print(
                f"WEATHER_PROGRESS runs={index}/{total} reused={reused_count} "
                f"downloaded={index - reused_count}",
                flush=True,
            )
    return results, unavailable


def build_summary(
    manifest: dict[str, Any], http_stats: dict[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    taxi = manifest["taxi"]
    weather = manifest["weather"]
    weather_failures = manifest.get("weather_failures", [])
    references = manifest["reference"]
    taxi_months = sorted(entry["month"] for entry in taxi)
    response_missing = Counter()
    predictor_missing = Counter()
    missing_slots: list[dict[str, Any]] = []
    missing_target_point_horizons = 0
    missing_variable_runs = 0
    for entry in weather:
        response_missing.update(entry.get("response_missing_values_by_variable", {}))
        predictor_missing.update(entry.get("required_predictor_missing_by_variable", {}))
        missing_target_point_horizons += entry.get("missing_required_target_point_horizon_count", 0)
        missing_variable_runs += bool(entry.get("missing_variable_names"))
        missing_slots.extend(entry.get("missing_required_predictor_slots", []))
    for entry in weather_failures:
        predictor_missing.update(entry.get("required_predictor_missing_by_variable", {}))
        missing_target_point_horizons += entry.get("missing_required_target_point_horizon_count", 0)
        missing_slots.extend(entry.get("missing_required_predictor_slots", []))

    if len(missing_slots) != sum(predictor_missing.values()):
        raise ValueError("detailed weather missing slots do not match aggregate missing counts")
    missing_by_run = Counter(slot["run_initialization_utc"] for slot in missing_slots)
    missing_by_target = Counter(slot["target_time_utc"] for slot in missing_slots)
    missing_by_horizon = Counter(str(slot["horizon_hours"]) for slot in missing_slots)
    missing_by_point = Counter(slot["point"] for slot in missing_slots)
    missing_by_variable = Counter(slot["variable"] for slot in missing_slots)

    raw_entries = taxi + weather + weather_failures + references
    raw_inventory = [
        {
            "artifact_type": entry["artifact_type"],
            "object_key": entry["object_key"],
            "s3a_uri": entry["s3a_uri"],
            "bytes": entry["bytes"],
            "sha256": entry["sha256"],
            "source_url": entry.get("source_url") or entry.get("request_url"),
        }
        for entry in raw_entries
    ]
    http_executions = [*manifest.get("execution_history", []), http_stats]
    cumulative_http = {
        "execution_count": len(http_executions),
        "logical_requests": sum(item.get("logical_requests", 0) for item in http_executions),
        "attempts": sum(item.get("attempts", 0) for item in http_executions),
        "retries": sum(item.get("retries", 0) for item in http_executions),
        "final_failure_count": sum(item.get("final_failure_count", 0) for item in http_executions),
        "final_failures": [
            failure
            for item in http_executions
            for failure in item.get("final_failures", [])
        ],
    }
    return {
        "summary_version": 1,
        "generated_at": utc_now(),
        "manifest_key": MANIFEST_KEY,
        "taxi": {
            "expected_month_count": 12,
            "present_month_count": len(taxi_months),
            "present_months": taxi_months,
            "missing_months": sorted(set(TAXI_MONTHS).difference(taxi_months)),
            "total_bytes": sum(entry["bytes"] for entry in taxi),
            "checksums": {entry["month"]: entry["sha256"] for entry in taxi},
            "reused_object_count": sum(entry.get("reused_existing_object", False) for entry in taxi),
        },
        "weather": {
            "expected_run_count": len(required_weather_plan()),
            "present_run_count": len(weather),
            "unavailable_run_count": len(weather_failures),
            "unavailable_runs": [entry["run_initialization_utc"] for entry in weather_failures],
            "request_artifact_count": len(weather) + len(weather_failures),
            "multi_coordinate_requests": True,
            "points_per_request": len(NYC_POINTS),
            "first_run": weather[0]["run_initialization_utc"] if weather else None,
            "last_run": weather[-1]["run_initialization_utc"] if weather else None,
            "total_bytes": sum(entry["bytes"] for entry in weather),
            "unavailable_response_bytes": sum(entry["bytes"] for entry in weather_failures),
            "reused_object_count": sum(entry.get("reused_existing_object", False) for entry in weather),
            "response_missing_values_by_variable": dict(sorted(response_missing.items())),
            "required_predictor_missing_by_variable": dict(sorted(predictor_missing.items())),
            "missing_predictor_slot_count": len(missing_slots),
            "missing_coverage_by_run": dict(sorted(missing_by_run.items())),
            "missing_coverage_by_target_hour": dict(sorted(missing_by_target.items())),
            "missing_coverage_by_horizon_hours": dict(sorted(missing_by_horizon.items())),
            "missing_coverage_by_point": dict(sorted(missing_by_point.items())),
            "missing_coverage_by_variable": dict(sorted(missing_by_variable.items())),
            "missing_target_hour_count": len(missing_by_target),
            "missing_target_hours": sorted(missing_by_target),
            "missing_slot_detail_location": MANIFEST_KEY,
            "missing_coverage_key": MISSING_COVERAGE_KEY,
            "missing_required_target_point_horizon_count": missing_target_point_horizons,
            "runs_missing_entire_variables": missing_variable_runs,
            "observed_or_reanalysis_predictors": False,
            "publication_lag_hours": PUBLICATION_LAG_HOURS,
        },
        "reference": {
            "expected_object_count": len(REFERENCE_SOURCES),
            "present_object_count": len(references),
            "total_bytes": sum(entry["bytes"] for entry in references),
        },
        "source_http": {
            "this_execution": http_stats,
            "cumulative": cumulative_http,
        },
        "raw_object_inventory": {
            "object_count": len(raw_inventory),
            "total_bytes": sum(entry["bytes"] for entry in raw_inventory),
            "inventory_key": INVENTORY_KEY,
        },
        "raw_artifacts_unchanged": all(
            entry.get("raw_object_sha256_verified", False) for entry in raw_entries
        ),
    }, raw_inventory


def validate_manifest_objects(
    store: ObjectStore,
    manifest: dict[str, Any],
    *,
    deep_verify: bool,
) -> dict[str, Any]:
    errors: list[dict[str, str]] = []
    checked_bytes = 0
    checked_objects = 0
    for section in ("taxi", "weather", "weather_failures", "reference"):
        for entry in manifest.get(section, []):
            checked_objects += 1
            checked_bytes += entry["bytes"]
            head = store.head(entry["object_key"])
            if head is None:
                errors.append({"object_key": entry["object_key"], "error": "missing"})
                continue
            if head["ContentLength"] != entry["bytes"]:
                errors.append({"object_key": entry["object_key"], "error": "size_mismatch"})
            if head.get("Metadata", {}).get("sha256") != entry["sha256"]:
                errors.append({"object_key": entry["object_key"], "error": "metadata_checksum_mismatch"})
            if deep_verify and store.sha256(entry["object_key"]) != entry["sha256"]:
                errors.append({"object_key": entry["object_key"], "error": "content_checksum_mismatch"})
    report = {
        "generated_at": utc_now(),
        "deep_checksum_verification": deep_verify,
        "checked_object_count": checked_objects,
        "checked_bytes": checked_bytes,
        "error_count": len(errors),
        "errors": errors,
        "passed": not errors,
    }
    if errors:
        raise ValueError(f"Bronze manifest validation failed with {len(errors)} errors")
    return report
