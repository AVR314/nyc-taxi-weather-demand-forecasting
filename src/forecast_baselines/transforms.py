"""Leakage-safe chronological splits and forecasting baselines."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


NYC_TIMEZONE = "America/New_York"
ROW_KEYS = ("location_id", "target_time_utc", "horizon_hours")
SPLIT_ORDER = ("train", "validation", "test")
SPLIT_BOUNDARIES = {
    "train": ("2025-01-01 00:00:00", "2025-09-01 00:00:00"),
    "validation": ("2025-09-01 00:00:00", "2025-11-01 00:00:00"),
    "test": ("2025-11-01 00:00:00", "2026-01-01 00:00:00"),
}
BASELINES = {
    "persistence": (
        "demand_at_cutoff",
        "demand_at_cutoff_source_time_utc",
    ),
    "previous_day_seasonal_naive": (
        "demand_same_local_hour_previous_day",
        "previous_day_source_time_utc",
    ),
    "previous_week_seasonal_naive": (
        "demand_same_local_hour_previous_week",
        "previous_week_source_time_utc",
    ),
}


def assign_chronological_split(frame: DataFrame) -> DataFrame:
    """Assign fixed half-open splits from America/New_York target wall time."""
    result = frame.withColumn(
        "split_target_local_time",
        F.from_utc_timestamp("target_time_utc", NYC_TIMEZONE),
    )
    split_column = F.lit(None).cast("string")
    for split_name in reversed(SPLIT_ORDER):
        start, end = SPLIT_BOUNDARIES[split_name]
        in_split = (
            (F.col("split_target_local_time") >= F.to_timestamp(F.lit(start)))
            & (F.col("split_target_local_time") < F.to_timestamp(F.lit(end)))
        )
        split_column = F.when(in_split, F.lit(split_name)).otherwise(split_column)
    return result.withColumn("split", split_column)


def paired_population(frame: DataFrame) -> DataFrame:
    """Return the single frozen key population shared by feature sets A and B."""
    return frame.filter(F.col("paired_evaluation_eligible"))


def exclusion_aggregates() -> list:
    """Aggregates for independent and mutually exclusive exclusion reasons."""
    demand_missing = ~F.col("demand_history_complete")
    weather_missing = F.col("any_weather_missing")
    paired = F.col("paired_evaluation_eligible")
    return [
        F.count(F.lit(1)).alias("candidate_rows"),
        F.sum(paired.cast("long")).alias("paired_rows"),
        F.sum((~paired).cast("long")).alias("excluded_rows"),
        F.sum(demand_missing.cast("long")).alias("demand_history_incomplete_rows"),
        F.sum(weather_missing.cast("long")).alias("weather_predictor_missing_rows"),
        F.sum((demand_missing & weather_missing).cast("long")).alias("both_missing_rows"),
        F.sum((demand_missing & ~weather_missing).cast("long")).alias(
            "demand_history_only_rows"
        ),
        F.sum((~demand_missing & weather_missing).cast("long")).alias(
            "weather_only_rows"
        ),
    ]


def baseline_predictions_long(frame: DataFrame) -> DataFrame:
    """Create one row per baseline without using target demand as a predictor."""
    pieces = []
    for name, (prediction_column, source_column) in BASELINES.items():
        pieces.append(
            frame.select(
                *ROW_KEYS,
                "split",
                F.lit(name).alias("baseline"),
                F.col("pickup_count").cast("double").alias("actual"),
                F.col(prediction_column).cast("double").alias("prediction"),
                F.col(source_column).alias("prediction_source_time_utc"),
                "prediction_cutoff_utc",
            )
        )
    result = pieces[0]
    for piece in pieces[1:]:
        result = result.unionByName(piece)
    return result


def metric_aggregates() -> list:
    absolute_error = F.abs(F.col("prediction") - F.col("actual"))
    squared_error = F.pow(F.col("prediction") - F.col("actual"), 2)
    return [
        F.count(F.lit(1)).alias("rows"),
        F.avg(absolute_error).alias("mae"),
        F.sqrt(F.avg(squared_error)).alias("rmse"),
    ]


def select_strongest_baseline(validation_metrics: list[dict]) -> dict:
    """Choose by overall validation MAE only, with name as deterministic tie-break."""
    if not validation_metrics:
        raise ValueError("Validation metrics are required")
    return min(validation_metrics, key=lambda row: (row["mae"], row["baseline"]))
