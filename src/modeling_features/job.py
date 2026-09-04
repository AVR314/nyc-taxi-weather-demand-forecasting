"""Build the leakage-safe Silver modeling feature dataset."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from modeling_features.transforms import (
    APPROVED_ZONE_IDS,
    CALENDAR_FEATURE_COLUMNS,
    DEMAND_FEATURE_COLUMNS,
    FEATURE_SET_A_COLUMNS,
    FEATURE_SET_B_COLUMNS,
    WEATHER_FEATURE_COLUMNS,
    WEATHER_VARIABLES,
    build_modeling_features,
)


BUCKET = "bigdata"
JOIN_INPUT = f"s3a://{BUCKET}/silver/demand_weather/records"
DEMAND_INPUT = f"s3a://{BUCKET}/silver/taxi_clean/hourly_demand"
FEATURE_OUTPUT = f"s3a://{BUCKET}/silver/modeling_features/records"
REPORT_OUTPUT = f"s3a://{BUCKET}/silver/manifests/modeling_features_report.json"


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("nyc-taxi-weather-modeling-features")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def duplicate_count(frame, keys: list[str]) -> int:
    return frame.groupBy(*keys).count().filter(F.col("count") > 1).count()


def write_json_object(spark: SparkSession, uri: str, value: dict) -> None:
    content = json.dumps(value, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = filesystem.create(path, True)
    try:
        stream.write(bytearray(content))
    finally:
        stream.close()


def output_inventory(spark: SparkSession) -> dict:
    path = spark._jvm.org.apache.hadoop.fs.Path(FEATURE_OUTPUT)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    iterator = filesystem.listFiles(path, True)
    objects = []
    while iterator.hasNext():
        status = iterator.next()
        objects.append(
            {
                "uri": status.getPath().toString(),
                "bytes": int(status.getLen()),
            }
        )
    return {
        "object_count": len(objects),
        "total_bytes": sum(item["bytes"] for item in objects),
        "objects": sorted(objects, key=lambda item: item["uri"]),
    }


def keyed_counts(frame, columns: list[str]) -> dict[str, int]:
    rows = frame.groupBy(*columns).count().orderBy(*columns).collect()
    return {
        "|".join(str(row[column]) for column in columns): int(row["count"])
        for row in rows
    }


def main() -> None:
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(JOIN_INPUT)
    silver_input_rows = silver.count()
    candidates = silver.filter(F.col("location_id").isin(*APPROVED_ZONE_IDS))
    candidate_rows = candidates.count()
    unavailable_target_rows = candidates.filter(~F.col("demand_available")).count()
    eligible_candidates = candidates.filter(F.col("demand_available"))

    history = spark.read.parquet(DEMAND_INPUT).filter(
        F.col("location_id").isin(*APPROVED_ZONE_IDS)
    )
    features = build_modeling_features(eligible_candidates, history)
    (
        features.repartition(48, "horizon_hours", "target_local_month")
        .write.mode("overwrite")
        .partitionBy("horizon_hours", "target_local_month")
        .parquet(FEATURE_OUTPUT)
    )
    output = spark.read.parquet(FEATURE_OUTPUT)
    output_rows = output.count()

    missing_expressions = [
        F.sum(F.col(column).isNull().cast("long")).alias(column)
        for column in DEMAND_FEATURE_COLUMNS
    ]
    missing_row = output.agg(*missing_expressions).first()
    demand_missing_counts = {column: int(missing_row[column]) for column in DEMAND_FEATURE_COLUMNS}
    weather_missing_rows = output.filter(F.col("any_weather_missing")).count()
    paired_eligible_rows = output.filter(F.col("paired_evaluation_eligible")).count()

    audit_sources = (
        "demand_at_cutoff_source_time_utc",
        "demand_cutoff_minus_1h_source_time_utc",
        "demand_cutoff_minus_2h_source_time_utc",
        "previous_day_source_time_utc",
        "previous_week_source_time_utc",
    )
    timestamp_violations = {
        column: output.filter(
            F.col(column).isNotNull() & (F.col(column) > F.col("prediction_cutoff_utc"))
        ).count()
        for column in audit_sources
    }
    rolling_end_violations = output.filter(
        F.col("rolling_window_end_utc").isNotNull()
        & (F.col("rolling_window_end_utc") != F.col("prediction_cutoff_utc"))
    ).count()
    key_duplicates = duplicate_count(
        output, ["location_id", "target_time_utc", "horizon_hours"]
    )

    if output_rows != candidate_rows - unavailable_target_rows:
        raise RuntimeError("Feature eligibility accounting mismatch")
    if any(timestamp_violations.values()) or rolling_end_violations:
        raise RuntimeError("Demand feature leakage validation failed")
    if key_duplicates:
        raise RuntimeError("Duplicate modeling feature keys detected")
    if set(FEATURE_SET_B_COLUMNS).difference(FEATURE_SET_A_COLUMNS) != set(
        WEATHER_FEATURE_COLUMNS
    ):
        raise RuntimeError("Feature set B differs from A by non-weather columns")
    if set(FEATURE_SET_A_COLUMNS).difference(output.columns):
        raise RuntimeError("Feature set A columns are missing from output")

    report = {
        "report_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "approved_zone_count": len(APPROVED_ZONE_IDS),
            "approved_zone_ids": list(APPROVED_ZONE_IDS),
            "horizons_hours": [1, 3, 6],
            "target_timezone_for_calendar": "America/New_York",
        },
        "eligibility": {
            "silver_input_rows": silver_input_rows,
            "excluded_unapproved_zone_rows": silver_input_rows - candidate_rows,
            "total_candidate_rows": candidate_rows,
            "excluded_unavailable_target_rows": unavailable_target_rows,
            "output_rows": output_rows,
            "other_excluded_rows": 0,
            "rows_with_any_weather_predictor_missing": weather_missing_rows,
            "rows_with_complete_demand_history": output.filter(
                F.col("demand_history_complete")
            ).count(),
            "paired_evaluation_eligible_rows": paired_eligible_rows,
            "demand_history_missing_by_feature": demand_missing_counts,
        },
        "counts": {
            "by_horizon": keyed_counts(output, ["horizon_hours"]),
            "by_local_month": keyed_counts(output, ["target_local_month"]),
            "by_zone": keyed_counts(output, ["location_id"]),
            "by_horizon_and_local_month": keyed_counts(
                output, ["horizon_hours", "target_local_month"]
            ),
        },
        "feature_sets": {
            "A_demand_calendar_only": list(FEATURE_SET_A_COLUMNS),
            "B_demand_calendar_plus_weather": list(FEATURE_SET_B_COLUMNS),
            "B_minus_A": list(WEATHER_FEATURE_COLUMNS),
            "weather_variables": list(WEATHER_VARIABLES),
            "no_imputation": True,
        },
        "validation": {
            "feature_source_timestamp_violations": timestamp_violations,
            "rolling_window_end_violations": rolling_end_violations,
            "primary_key_duplicate_count": key_duplicates,
            "feature_set_difference_is_weather_only": True,
            "target_demand_used_as_feature": False,
        },
        "schema": output.schema.jsonValue(),
        "object_inventory": output_inventory(spark),
    }
    write_json_object(spark, REPORT_OUTPUT, report)
    local_output = Path("/opt/project/output")
    local_output.mkdir(parents=True, exist_ok=True)
    (local_output / "modeling_features_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        "MODELING_FEATURES_OK "
        f"candidate={candidate_rows} unavailable={unavailable_target_rows} "
        f"output={output_rows} weather_missing={weather_missing_rows} "
        f"paired={paired_eligible_rows}",
        flush=True,
    )
    spark.stop()


if __name__ == "__main__":
    main()
