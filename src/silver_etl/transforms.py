"""Deterministic, testable transformations used by the Silver Spark job."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import asin, cos, radians, sin, sqrt
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession, functions as F


NYC_TIMEZONE = "America/New_York"
LOCAL_YEAR_START = "2025-01-01 00:00:00"
LOCAL_YEAR_END_EXCLUSIVE = "2026-01-01 00:00:00"
TARGET_START_UTC = datetime(2025, 1, 1, 5, tzinfo=timezone.utc)
TARGET_END_EXCLUSIVE_UTC = datetime(2026, 1, 1, 5, tzinfo=timezone.utc)
SPRING_GAP_START = "2025-03-09 02:00:00"
SPRING_GAP_END = "2025-03-09 03:00:00"
FALL_FOLD_START = "2025-11-02 01:00:00"
FALL_FOLD_END = "2025-11-02 02:00:00"
FALL_UNAVAILABLE_TARGETS_UTC = (
    datetime(2025, 11, 2, 5, tzinfo=timezone.utc),
    datetime(2025, 11, 2, 6, tzinfo=timezone.utc),
)
FORECAST_HORIZONS_HOURS = (1, 3, 6)
PUBLICATION_LAG_HOURS = 6
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
    ("lower_manhattan", 40.7128, -74.0060),
    ("bronx", 40.8448, -73.8648),
    ("brooklyn", 40.6782, -73.9442),
    ("jfk_queens", 40.6413, -73.7781),
    ("staten_island", 40.5795, -74.1502),
)
FIVE_BOROUGHS = ("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island")


def classify_taxi(taxi: DataFrame, zone_lookup: DataFrame) -> DataFrame:
    """Classify rows without letting Spark resolve DST anomalies implicitly."""
    zones = zone_lookup.select(
        F.col("LocationID").cast("int").alias("lookup_location_id"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone_name"),
        F.col("service_zone").alias("pickup_service_zone"),
    )
    joined = taxi.join(
        F.broadcast(zones),
        taxi.PULocationID.cast("int") == zones.lookup_location_id,
        "left",
    )
    pickup = F.col("tpep_pickup_datetime")
    reason = (
        F.when(pickup.isNull(), F.lit("missing_pickup_timestamp"))
        .when(
            (pickup < F.to_timestamp(F.lit(LOCAL_YEAR_START)))
            | (pickup >= F.to_timestamp(F.lit(LOCAL_YEAR_END_EXCLUSIVE))),
            F.lit("outside_local_year_2025"),
        )
        .when(
            (pickup >= F.to_timestamp(F.lit(SPRING_GAP_START)))
            & (pickup < F.to_timestamp(F.lit(SPRING_GAP_END))),
            F.lit("dst_nonexistent_spring_forward"),
        )
        .when(
            (pickup >= F.to_timestamp(F.lit(FALL_FOLD_START)))
            & (pickup < F.to_timestamp(F.lit(FALL_FOLD_END))),
            F.lit("dst_ambiguous_fall_back"),
        )
        .when(F.col("PULocationID").isNull(), F.lit("missing_pickup_zone"))
        .when(F.col("lookup_location_id").isNull(), F.lit("invalid_pickup_zone"))
        .when(~F.col("pickup_borough").isin(*FIVE_BOROUGHS), F.lit("outside_five_boroughs"))
    )
    classified = joined.withColumn("record_reason", reason).withColumn(
        "record_status",
        F.when(F.col("record_reason").startswith("dst_"), F.lit("quarantined"))
        .when(F.col("record_reason").isNotNull(), F.lit("rejected"))
        .otherwise(F.lit("accepted")),
    )
    return (
        classified.withColumn(
            "pickup_time_utc",
            F.when(
                F.col("record_status") == "accepted",
                F.to_utc_timestamp(pickup, NYC_TIMEZONE),
            ).cast("timestamp"),
        )
        .withColumn("pickup_hour_utc", F.date_trunc("hour", F.col("pickup_time_utc")))
        .drop("lookup_location_id")
    )


def canonical_hour_axis(spark: SparkSession) -> DataFrame:
    """Create the 8,760 real UTC instants in local calendar year 2025."""
    return spark.sql(
        "SELECT explode(sequence("
        "timestamp'2025-01-01 05:00:00', "
        "timestamp'2026-01-01 04:00:00', "
        "interval 1 hour)) AS target_time_utc"
    )


def complete_demand_grid(
    accepted: DataFrame, five_borough_zones: DataFrame, hours: DataFrame
) -> DataFrame:
    """Aggregate pickup demand and make true zero hours explicit."""
    counts = accepted.groupBy(
        F.col("PULocationID").cast("int").alias("location_id"),
        "pickup_hour_utc",
    ).agg(F.count(F.lit(1)).cast("long").alias("observed_pickup_count"))
    grid = five_borough_zones.select(F.col("LocationID").cast("int").alias("location_id")).distinct().crossJoin(hours)
    joined = grid.join(
        counts,
        (grid.location_id == counts.location_id)
        & (grid.target_time_utc == counts.pickup_hour_utc),
        "left",
    ).select(grid.location_id, grid.target_time_utc, "observed_pickup_count")
    unavailable = F.col("target_time_utc").isin(*FALL_UNAVAILABLE_TARGETS_UTC)
    return (
        joined.withColumn("demand_available", ~unavailable)
        .withColumn(
            "pickup_count",
            F.when(unavailable, F.lit(None).cast("long")).otherwise(
                F.coalesce(F.col("observed_pickup_count"), F.lit(0).cast("long"))
            ),
        )
        .drop("observed_pickup_count")
    )


def floor_cycle(value: datetime) -> datetime:
    return value.replace(hour=(value.hour // 6) * 6, minute=0, second=0, microsecond=0)


def weather_plan_rows() -> list[tuple[datetime, datetime, int, datetime]]:
    rows: list[tuple[datetime, datetime, int, datetime]] = []
    target = TARGET_START_UTC
    while target < TARGET_END_EXCLUSIVE_UTC:
        for horizon in FORECAST_HORIZONS_HOURS:
            cutoff = target - timedelta(hours=horizon)
            run = floor_cycle(cutoff - timedelta(hours=PUBLICATION_LAG_HOURS))
            rows.append((run, target, horizon, cutoff))
        target += timedelta(hours=1)
    return rows


def expected_weather_grid(
    spark: SparkSession, weather_points: DataFrame
) -> DataFrame:
    plan = spark.createDataFrame(
        weather_plan_rows(),
        "run_initialization_utc timestamp, target_time_utc timestamp, "
        "horizon_hours int, prediction_cutoff_utc timestamp",
    )
    return plan.crossJoin(weather_points.select("weather_point"))


def complete_weather_records(expected: DataFrame, parsed: DataFrame) -> DataFrame:
    """Left join raw forecast values so unavailable/null predictors stay explicit."""
    keys = ["run_initialization_utc", "target_time_utc", "weather_point"]
    selected = parsed.select(
        *keys,
        "provider_latitude",
        "provider_longitude",
        "provider_timezone",
        "provider_utc_offset_seconds",
        *WEATHER_VARIABLES,
        F.lit(True).alias("source_response_available"),
    )
    result = expected.join(selected, keys, "left")
    for variable in WEATHER_VARIABLES:
        result = result.withColumn(f"{variable}_missing", F.col(variable).isNull())
    missing_columns = [F.col(f"{variable}_missing") for variable in WEATHER_VARIABLES]
    any_missing = missing_columns[0]
    for column in missing_columns[1:]:
        any_missing = any_missing | column
    return (
        result.withColumn(
            "source_response_available",
            F.coalesce(F.col("source_response_available"), F.lit(False)),
        )
        .withColumn("any_weather_missing", any_missing)
        .withColumn("publication_lag_hours", F.lit(PUBLICATION_LAG_HOURS))
    )


def join_demand_weather(
    demand: DataFrame, zone_weather_map: DataFrame, weather: DataFrame
) -> DataFrame:
    mapped = demand.join(zone_weather_map, "location_id", "inner")
    return mapped.join(
        weather,
        ["target_time_utc", "weather_point"],
        "left",
    )


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    earth_radius_km = 6371.0088
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    value = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * earth_radius_km * asin(sqrt(value))


def nearest_weather_point(latitude: float, longitude: float) -> tuple[str, float]:
    distances = sorted(
        (haversine_km(latitude, longitude, point_lat, point_lon), slug)
        for slug, point_lat, point_lon in NYC_POINTS
    )
    distance, slug = distances[0]
    return slug, distance
