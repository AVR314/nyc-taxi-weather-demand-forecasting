"""Leakage-safe Spark transformations for the modeling feature dataset."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


NYC_TIMEZONE = "America/New_York"
APPROVED_ZONE_IDS = (
    237, 161, 132, 236, 186, 230, 162, 142, 170, 234, 138, 68, 163, 79,
    239, 48, 249, 164, 141, 107, 140, 246, 238, 263, 229, 90, 114, 113,
    231, 100, 262, 43, 148, 144, 143, 137, 233, 158, 211, 151, 75, 87,
    50, 13, 166, 261, 125, 41, 74, 88, 4, 42, 70, 24, 232, 209, 45,
    224, 145, 244, 255, 7, 116, 226, 61, 256, 37, 112, 65, 97, 33,
    80, 66, 152,
)
WEATHER_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "snowfall",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
)
DEMAND_FEATURE_COLUMNS = (
    "demand_at_cutoff",
    "demand_cutoff_minus_1h",
    "demand_cutoff_minus_2h",
    "demand_same_local_hour_previous_day",
    "demand_same_local_hour_previous_week",
    "demand_trailing_mean_3h",
    "demand_trailing_mean_6h",
    "demand_trailing_mean_24h",
    "demand_trailing_stddev_24h",
)
CALENDAR_FEATURE_COLUMNS = (
    "target_local_hour",
    "target_local_day_of_week",
    "target_local_is_weekend",
    "target_local_month",
)
WEATHER_FEATURE_COLUMNS = tuple(WEATHER_VARIABLES) + tuple(
    f"{variable}_missing" for variable in WEATHER_VARIABLES
) + ("any_weather_missing", "source_response_available")
FEATURE_SET_A_COLUMNS = DEMAND_FEATURE_COLUMNS + CALENDAR_FEATURE_COLUMNS
FEATURE_SET_B_COLUMNS = FEATURE_SET_A_COLUMNS + WEATHER_FEATURE_COLUMNS


def history_at_each_cutoff(demand_history: DataFrame) -> DataFrame:
    """Compute zone-specific lag/rolling values for every real cutoff instant."""
    base = demand_history.select(
        "location_id",
        F.col("target_time_utc").alias("history_time_utc"),
        "demand_available",
        "pickup_count",
    ).withColumn("history_epoch", F.col("history_time_utc").cast("long"))
    ordered = Window.partitionBy("location_id").orderBy("history_epoch")

    result = (
        base.withColumn("demand_at_cutoff", F.col("pickup_count"))
        .withColumn("demand_cutoff_minus_1h", F.lag("pickup_count", 1).over(ordered))
        .withColumn("demand_cutoff_minus_2h", F.lag("pickup_count", 2).over(ordered))
    )
    for hours in (3, 6, 24):
        window = ordered.rowsBetween(-(hours - 1), 0)
        complete = F.count("pickup_count").over(window) == hours
        result = result.withColumn(
            f"demand_trailing_mean_{hours}h",
            F.when(complete, F.avg("pickup_count").over(window)),
        )
        if hours == 24:
            result = result.withColumn(
                "demand_trailing_stddev_24h",
                F.when(complete, F.stddev_pop("pickup_count").over(window)),
            )

    return result.select(
        "location_id",
        F.col("history_time_utc").alias("prediction_cutoff_utc"),
        *[column for column in DEMAND_FEATURE_COLUMNS if "same_local_hour" not in column],
        F.when(F.col("demand_at_cutoff").isNotNull(), F.col("history_time_utc")).alias(
            "demand_at_cutoff_source_time_utc"
        ),
        F.when(
            F.col("demand_cutoff_minus_1h").isNotNull(),
            F.col("history_time_utc") - F.expr("INTERVAL 1 HOUR"),
        ).alias("demand_cutoff_minus_1h_source_time_utc"),
        F.when(
            F.col("demand_cutoff_minus_2h").isNotNull(),
            F.col("history_time_utc") - F.expr("INTERVAL 2 HOURS"),
        ).alias("demand_cutoff_minus_2h_source_time_utc"),
        F.when(
            F.col("demand_trailing_mean_3h").isNotNull(),
            F.col("history_time_utc") - F.expr("INTERVAL 2 HOURS"),
        ).alias("trailing_3h_start_utc"),
        F.when(
            F.col("demand_trailing_mean_6h").isNotNull(),
            F.col("history_time_utc") - F.expr("INTERVAL 5 HOURS"),
        ).alias("trailing_6h_start_utc"),
        F.when(
            F.col("demand_trailing_mean_24h").isNotNull(),
            F.col("history_time_utc") - F.expr("INTERVAL 23 HOURS"),
        ).alias("trailing_24h_start_utc"),
        F.when(
            F.col("demand_trailing_mean_3h").isNotNull()
            | F.col("demand_trailing_mean_6h").isNotNull()
            | F.col("demand_trailing_mean_24h").isNotNull(),
            F.col("history_time_utc"),
        ).alias("rolling_window_end_utc"),
    )


def local_hour_lookup(demand_history: DataFrame) -> DataFrame:
    """Create a unique local-wall-hour lookup; folds remain intentionally unusable."""
    grouped = (
        demand_history.withColumn(
            "local_wall_hour",
            F.from_utc_timestamp("target_time_utc", NYC_TIMEZONE),
        )
        .groupBy("location_id", "local_wall_hour")
        .agg(
            F.count(F.lit(1)).alias("local_instant_count"),
            F.sum(F.col("demand_available").cast("int")).alias("available_instant_count"),
            F.max("pickup_count").alias("local_pickup_count"),
            F.min("target_time_utc").alias("local_source_time_utc"),
        )
    )
    usable = (F.col("local_instant_count") == 1) & (F.col("available_instant_count") == 1)
    return grouped.select(
        "location_id",
        "local_wall_hour",
        F.when(usable, F.col("local_pickup_count")).alias("local_pickup_count"),
        F.when(usable, F.col("local_source_time_utc")).alias("local_source_time_utc"),
    )


def build_modeling_features(candidates: DataFrame, demand_history: DataFrame) -> DataFrame:
    """Attach demand history and calendar features without crossing the cutoff."""
    target = (
        candidates.withColumn(
            "prediction_cutoff_utc",
            F.expr("target_time_utc - make_interval(0, 0, 0, 0, horizon_hours)"),
        )
        .withColumn(
            "target_local_time",
            F.from_utc_timestamp("target_time_utc", NYC_TIMEZONE),
        )
        .withColumn("target_local_hour", F.hour("target_local_time"))
        .withColumn("target_local_day_of_week", F.dayofweek("target_local_time"))
        .withColumn(
            "target_local_is_weekend",
            F.dayofweek("target_local_time").isin(1, 7),
        )
        .withColumn("target_local_month", F.month("target_local_time"))
        .withColumn("previous_day_local_wall_hour", F.expr("target_local_time - INTERVAL 1 DAY"))
        .withColumn("previous_week_local_wall_hour", F.expr("target_local_time - INTERVAL 7 DAYS"))
    )

    cutoff = history_at_each_cutoff(demand_history)
    result = target.join(cutoff, ["location_id", "prediction_cutoff_utc"], "left")
    lookup = local_hour_lookup(demand_history)
    previous_day = lookup.select(
        "location_id",
        F.col("local_wall_hour").alias("previous_day_local_wall_hour"),
        F.col("local_pickup_count").alias("demand_same_local_hour_previous_day"),
        F.col("local_source_time_utc").alias("previous_day_source_time_utc"),
    )
    previous_week = lookup.select(
        "location_id",
        F.col("local_wall_hour").alias("previous_week_local_wall_hour"),
        F.col("local_pickup_count").alias("demand_same_local_hour_previous_week"),
        F.col("local_source_time_utc").alias("previous_week_source_time_utc"),
    )
    result = result.join(
        previous_day, ["location_id", "previous_day_local_wall_hour"], "left"
    ).join(previous_week, ["location_id", "previous_week_local_wall_hour"], "left")

    demand_complete = F.lit(True)
    for column in DEMAND_FEATURE_COLUMNS:
        demand_complete = demand_complete & F.col(column).isNotNull()
    return (
        result.withColumn("demand_history_complete", demand_complete)
        .withColumn("weather_predictors_complete", ~F.col("any_weather_missing"))
        .withColumn(
            "paired_evaluation_eligible",
            F.col("demand_history_complete") & F.col("weather_predictors_complete"),
        )
    )
