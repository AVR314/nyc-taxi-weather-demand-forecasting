"""Freeze chronological splits and evaluate non-ML demand baselines."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from forecast_baselines.transforms import (
    BASELINES,
    NYC_TIMEZONE,
    ROW_KEYS,
    SPLIT_BOUNDARIES,
    SPLIT_ORDER,
    assign_chronological_split,
    baseline_predictions_long,
    exclusion_aggregates,
    metric_aggregates,
    paired_population,
    select_strongest_baseline,
)


BUCKET = "bigdata"
FEATURE_INPUT = f"s3a://{BUCKET}/silver/modeling_features/records"
REPORT_OUTPUT = f"s3a://{BUCKET}/silver/manifests/chronological_splits_baselines_report.json"


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("nyc-taxi-weather-chronological-baselines")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def write_json_object(spark: SparkSession, uri: str, value: dict) -> None:
    content = json.dumps(value, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = filesystem.create(path, True)
    try:
        stream.write(bytearray(content))
    finally:
        stream.close()


def row_dict(row, fields: tuple[str, ...] | list[str]) -> dict:
    value = {}
    for field in fields:
        item = row[field]
        if isinstance(item, datetime):
            item = item.isoformat()
        elif isinstance(item, float):
            item = float(item)
        elif isinstance(item, int):
            item = int(item)
        value[field] = item
    return value


def duplicate_count(frame) -> int:
    return frame.groupBy(*ROW_KEYS).count().filter(F.col("count") > 1).count()


def split_overlap_counts(frame) -> dict[str, int]:
    targets = {
        name: frame.filter(F.col("split") == name)
        .select("target_time_utc")
        .distinct()
        for name in SPLIT_ORDER
    }
    return {
        f"{left}|{right}": targets[left].join(targets[right], "target_time_utc").count()
        for index, left in enumerate(SPLIT_ORDER)
        for right in SPLIT_ORDER[index + 1 :]
    }


def main() -> None:
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    assigned = assign_chronological_split(spark.read.parquet(FEATURE_INPUT)).cache()
    input_rows = assigned.count()
    outside_split_rows = assigned.filter(F.col("split").isNull()).count()
    duplicate_keys = duplicate_count(assigned)
    paired_definition_mismatches = assigned.filter(
        F.col("paired_evaluation_eligible")
        != (F.col("demand_history_complete") & ~F.col("any_weather_missing"))
    ).count()

    exclusion_rows = (
        assigned.groupBy("split", "horizon_hours")
        .agg(*exclusion_aggregates())
        .orderBy("split", "horizon_hours")
        .collect()
    )
    exclusion_fields = (
        "split",
        "horizon_hours",
        "candidate_rows",
        "paired_rows",
        "excluded_rows",
        "demand_history_incomplete_rows",
        "weather_predictor_missing_rows",
        "both_missing_rows",
        "demand_history_only_rows",
        "weather_only_rows",
    )
    split_counts = [row_dict(row, exclusion_fields) for row in exclusion_rows]

    paired = paired_population(assigned).cache()
    paired_rows = paired.count()
    paired_duplicate_keys = duplicate_count(paired)
    overlap_counts = split_overlap_counts(assigned)

    ranges = {
        row["split"]: row_dict(
            row,
            [
                "min_target_local_time",
                "max_target_local_time",
                "min_target_time_utc",
                "max_target_time_utc",
            ],
        )
        for row in assigned.groupBy("split")
        .agg(
            F.min("split_target_local_time").alias("min_target_local_time"),
            F.max("split_target_local_time").alias("max_target_local_time"),
            F.min("target_time_utc").alias("min_target_time_utc"),
            F.max("target_time_utc").alias("max_target_time_utc"),
        )
        .collect()
    }
    strict_order = all(
        ranges[SPLIT_ORDER[index]]["max_target_local_time"]
        < ranges[SPLIT_ORDER[index + 1]]["min_target_local_time"]
        for index in range(len(SPLIT_ORDER) - 1)
    )

    predictions = baseline_predictions_long(
        paired.filter(F.col("split").isin("validation", "test"))
    ).cache()
    prediction_rows = predictions.count()
    prediction_null_rows = predictions.filter(
        F.col("prediction").isNull() | F.col("actual").isNull()
    ).count()
    source_violations = {
        row["baseline"]: int(row["violations"])
        for row in predictions.groupBy("baseline")
        .agg(
            F.sum(
                (
                    F.col("prediction_source_time_utc").isNull()
                    | (F.col("prediction_source_time_utc") > F.col("prediction_cutoff_utc"))
                ).cast("long")
            ).alias("violations")
        )
        .collect()
    }

    metric_fields = ("split", "baseline", "rows", "mae", "rmse")
    overall_metrics = [
        row_dict(row, metric_fields)
        for row in predictions.groupBy("split", "baseline")
        .agg(*metric_aggregates())
        .orderBy("split", "baseline")
        .collect()
    ]
    horizon_metric_fields = (
        "split",
        "horizon_hours",
        "baseline",
        "rows",
        "mae",
        "rmse",
    )
    horizon_metrics = [
        row_dict(row, horizon_metric_fields)
        for row in predictions.groupBy("split", "horizon_hours", "baseline")
        .agg(*metric_aggregates())
        .orderBy("split", "horizon_hours", "baseline")
        .collect()
    ]

    zone_mae = predictions.groupBy("split", "baseline", "location_id").agg(
        F.avg(F.abs(F.col("prediction") - F.col("actual"))).alias("zone_mae")
    )
    zone_summary_fields = (
        "split",
        "baseline",
        "zones",
        "mean_zone_mae",
        "min_zone_mae",
        "p25_zone_mae",
        "median_zone_mae",
        "p75_zone_mae",
        "max_zone_mae",
    )
    zone_summaries = [
        row_dict(row, zone_summary_fields)
        for row in zone_mae.groupBy("split", "baseline")
        .agg(
            F.count(F.lit(1)).alias("zones"),
            F.avg("zone_mae").alias("mean_zone_mae"),
            F.min("zone_mae").alias("min_zone_mae"),
            F.percentile_approx("zone_mae", 0.25, 10000).alias("p25_zone_mae"),
            F.percentile_approx("zone_mae", 0.5, 10000).alias("median_zone_mae"),
            F.percentile_approx("zone_mae", 0.75, 10000).alias("p75_zone_mae"),
            F.max("zone_mae").alias("max_zone_mae"),
        )
        .orderBy("split", "baseline")
        .collect()
    ]

    validation_overall = [
        row for row in overall_metrics if row["split"] == "validation"
    ]
    selected_baseline = select_strongest_baseline(validation_overall)

    if outside_split_rows or duplicate_keys or paired_duplicate_keys:
        raise RuntimeError("Split coverage or row-key validation failed")
    if paired_definition_mismatches:
        raise RuntimeError("Stored paired population differs from its approved definition")
    if not strict_order or any(overlap_counts.values()):
        raise RuntimeError("Chronological split ordering or overlap validation failed")
    if prediction_null_rows or any(source_violations.values()):
        raise RuntimeError("Baseline completeness or leakage validation failed")
    if prediction_rows != sum(
        row["paired_rows"] * len(BASELINES)
        for row in split_counts
        if row["split"] in ("validation", "test")
    ):
        raise RuntimeError("Baseline row accounting mismatch")

    report = {
        "report_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input": {
            "feature_uri": FEATURE_INPUT,
            "rows": input_rows,
            "full_feature_dataset_duplicated": False,
        },
        "protocol": {
            "timezone": NYC_TIMEZONE,
            "half_open_splits": {
                name: {"start_local_inclusive": bounds[0], "end_local_exclusive": bounds[1]}
                for name, bounds in SPLIT_BOUNDARIES.items()
            },
            "random_split": False,
            "primary_population": "paired_evaluation_eligible == true",
            "imputation": False,
            "selection_metric": "overall validation MAE",
            "test_used_for_selection": False,
        },
        "split_counts_and_exclusions": split_counts,
        "paired_population": {
            "rows": paired_rows,
            "feature_set_A_rows": paired_rows,
            "feature_set_B_rows": paired_rows,
            "key_symmetric_difference": 0,
        },
        "baselines": {
            "definitions": {
                name: {"prediction_column": columns[0], "source_time_column": columns[1]}
                for name, columns in BASELINES.items()
            },
            "overall_metrics": overall_metrics,
            "metrics_by_horizon": horizon_metrics,
            "by_zone_mae_summary": zone_summaries,
            "selected_on_validation_mae": selected_baseline,
        },
        "validation": {
            "outside_split_rows": outside_split_rows,
            "split_ranges": ranges,
            "strict_split_order": strict_order,
            "timestamp_overlap_counts": overlap_counts,
            "duplicate_row_keys": duplicate_keys,
            "duplicate_paired_row_keys": paired_duplicate_keys,
            "paired_definition_mismatches": paired_definition_mismatches,
            "baseline_prediction_or_target_null_rows": prediction_null_rows,
            "baseline_source_timestamp_violations": source_violations,
            "target_demand_used_as_baseline_predictor": False,
        },
    }
    write_json_object(spark, REPORT_OUTPUT, report)
    local_output = Path("/opt/project/output")
    local_output.mkdir(parents=True, exist_ok=True)
    (local_output / "chronological_splits_baselines_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        "BASELINES_OK "
        f"input={input_rows} paired={paired_rows} "
        f"selected={selected_baseline['baseline']} "
        f"validation_mae={selected_baseline['mae']:.6f}",
        flush=True,
    )
    predictions.unpersist()
    paired.unpersist()
    assigned.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
