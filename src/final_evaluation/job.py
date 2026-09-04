"""Phase 5D: frozen final TEST evaluation of the Phase 5C-selected model.

Model family, hyperparameters, feature contracts, and preprocessing design are
frozen from Phase 5C validation-only selection (see
data/silver/ml_candidate_selection_report.json and
docs/ml_selection_validation.md). Nothing here may change based on TEST
results: this job only refits the already-frozen configuration on
TRAIN+VALIDATION and scores it once on the frozen TEST partitions.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml.regression import LinearRegression
from pyspark.sql import DataFrame, SparkSession, functions as F

from forecast_baselines.transforms import assign_chronological_split
from final_evaluation.transforms import (
    ALL_MONTHS,
    FROZEN_BASELINE_TEST_METRICS,
    FROZEN_CONFIGS,
    TEST_MONTHS,
    all_month_partition_paths,
)
from ml_selection.transforms import (
    FEATURE_CONTRACTS,
    FEATURE_ROOT,
    ROW_KEYS,
    categorical_vocabularies,
    clip_nonnegative,
    prepare_model_columns,
    preprocessing_pipeline,
    validation_metric_aggregates,
)


BUCKET = "bigdata"
HORIZONS = (1, 3, 6)
MODEL_FAMILY = "regularized_linear_regression"
GOLD_PREDICTIONS_ROOT = f"s3a://{BUCKET}/gold/predictions"
GOLD_METRICS_OUTPUT = f"s3a://{BUCKET}/gold/metrics/final_test_evaluation_report.json"
TEST_START_LOCAL_INCLUSIVE = "2025-11-01 00:00:00"


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("nyc-taxi-weather-final-test-evaluation")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
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


def duplicate_count(frame: DataFrame) -> int:
    return frame.groupBy(*ROW_KEYS).count().filter(F.col("count") > 1).count()


def read_all_months_horizon(spark: SparkSession, horizon: int) -> DataFrame:
    paths = all_month_partition_paths(horizon)
    assert len(paths) == len(ALL_MONTHS)
    return (
        spark.read.option("basePath", FEATURE_ROOT)
        .parquet(*paths)
        .transform(assign_chronological_split)
        .filter(F.col("paired_evaluation_eligible"))
    )


def build_frozen_estimator(feature_set: str, horizon: int) -> LinearRegression:
    parameters = FROZEN_CONFIGS[horizon][feature_set]
    return LinearRegression(
        featuresCol="features",
        labelCol="pickup_count",
        predictionCol="raw_prediction",
        regParam=parameters["reg_param"],
        elasticNetParam=parameters["elastic_net_param"],
        maxIter=parameters["max_iter"],
        solver="normal",
        standardization=True,
    )


def main() -> None:
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    per_horizon_metrics: list[dict] = []
    preprocessing_audit: list[dict] = []
    population_audit: list[dict] = []
    weather_deltas: list[dict] = []
    prediction_frames: list[DataFrame] = []

    for horizon in HORIZONS:
        population = read_all_months_horizon(spark, horizon).cache()
        train_val = population.filter(F.col("split").isin("train", "validation")).cache()
        test = population.filter(F.col("split") == "test").cache()

        train_val_rows = train_val.count()
        test_rows = test.count()
        population_rows = population.count()
        duplicate_keys = duplicate_count(population)
        forbidden_train_val_rows = train_val.filter(
            F.col("split_target_local_time") >= F.to_timestamp(F.lit(TEST_START_LOCAL_INCLUSIVE))
        ).count()
        non_test_month_rows = test.filter(
            ~F.col("target_local_month").isin(*TEST_MONTHS)
        ).count()
        if (
            duplicate_keys
            or forbidden_train_val_rows
            or non_test_month_rows
            or population_rows != train_val_rows + test_rows
        ):
            raise RuntimeError("TRAIN+VALIDATION/TEST population validation failed")

        feature_set_predictions: dict[str, DataFrame] = {}
        feature_set_test_rows: dict[str, int] = {}
        shared_vocabularies: dict[str, dict[str, list[str]]] = {}

        for feature_set, contract in FEATURE_CONTRACTS.items():
            prepared_train_val = prepare_model_columns(train_val, contract)
            prepared_test = prepare_model_columns(test, contract)
            preprocessor = preprocessing_pipeline(contract).fit(prepared_train_val)
            vocabularies = categorical_vocabularies(preprocessor, contract)
            shared_vocabularies[feature_set] = vocabularies

            refit_train = preprocessor.transform(prepared_train_val).select(
                *ROW_KEYS, "pickup_count", "features"
            ).cache()
            transformed_test = preprocessor.transform(prepared_test).select(
                *ROW_KEYS, "pickup_count", "features"
            ).cache()
            refit_train_rows = refit_train.count()
            transformed_test_rows = transformed_test.count()
            if refit_train_rows != train_val_rows or transformed_test_rows != test_rows:
                raise RuntimeError("Preprocessing changed the A/B population")

            preprocessing_audit.append(
                {
                    "horizon_hours": horizon,
                    "feature_set": feature_set,
                    "fit_split": "train+validation",
                    "fit_rows": train_val_rows,
                    "test_transform_rows": test_rows,
                    "categorical_columns": list(contract.categorical_columns),
                    "numeric_columns": list(contract.numeric_columns),
                }
            )

            model = build_frozen_estimator(feature_set, horizon).fit(refit_train)
            scored = clip_nonnegative(model.transform(transformed_test)).select(
                *ROW_KEYS, "pickup_count", "raw_prediction", "prediction"
            ).cache()
            metrics = scored.agg(*validation_metric_aggregates()).first()
            if int(metrics["prediction_rows"]) != test_rows or metrics["invalid_prediction_rows"]:
                raise RuntimeError("TEST prediction cardinality or validity failed")

            hyperparameters = FROZEN_CONFIGS[horizon][feature_set]
            per_horizon_metrics.append(
                {
                    "horizon_hours": horizon,
                    "feature_set": feature_set,
                    "model_family": MODEL_FAMILY,
                    "hyperparameters": dict(hyperparameters),
                    "test_rows": int(metrics["prediction_rows"]),
                    "raw_negative_prediction_rows": int(metrics["raw_negative_prediction_rows"]),
                    "clipped_prediction_rows": int(metrics["raw_negative_prediction_rows"]),
                    "mae": float(metrics["mae"]),
                    "rmse": float(metrics["rmse"]),
                }
            )
            feature_set_predictions[feature_set] = scored.withColumn(
                "feature_set", F.lit(feature_set)
            ).select(
                "location_id",
                "target_time_utc",
                "horizon_hours",
                F.col("pickup_count").alias("actual"),
                "prediction",
                "feature_set",
            )
            feature_set_test_rows[feature_set] = test_rows
            refit_train.unpersist()
            transformed_test.unpersist()

        for column in ("location_id", "target_local_hour", "target_local_day_of_week", "target_local_month"):
            if shared_vocabularies["A"][column] != shared_vocabularies["B"][column]:
                raise RuntimeError(f"A/B categorical vocabulary mismatch: {column}")

        a_keys = feature_set_predictions["A"].select(*ROW_KEYS)
        b_keys = feature_set_predictions["B"].select(*ROW_KEYS)
        key_symmetric_difference = (
            a_keys.exceptAll(b_keys).count() + b_keys.exceptAll(a_keys).count()
        )
        if key_symmetric_difference:
            raise RuntimeError("A/B TEST key populations are not identical")

        a_metrics = next(
            row for row in per_horizon_metrics
            if row["horizon_hours"] == horizon and row["feature_set"] == "A"
        )
        b_metrics = next(
            row for row in per_horizon_metrics
            if row["horizon_hours"] == horizon and row["feature_set"] == "B"
        )
        mae_delta = a_metrics["mae"] - b_metrics["mae"]
        weather_deltas.append(
            {
                "horizon_hours": horizon,
                "model_family": MODEL_FAMILY,
                "A_mae": a_metrics["mae"],
                "B_mae": b_metrics["mae"],
                "absolute_mae_delta": mae_delta,
                "percent_mae_delta": 100.0 * mae_delta / a_metrics["mae"],
            }
        )

        baseline = FROZEN_BASELINE_TEST_METRICS[horizon]
        population_audit.append(
            {
                "horizon_hours": horizon,
                "physical_month_partitions_read": list(ALL_MONTHS),
                "train_validation_rows": train_val_rows,
                "test_rows": test_rows,
                "test_rows_feature_set_A": feature_set_test_rows["A"],
                "test_rows_feature_set_B": feature_set_test_rows["B"],
                "A_B_key_symmetric_difference": key_symmetric_difference,
                "duplicate_row_keys": duplicate_keys,
                "forbidden_train_validation_rows_in_test_period": forbidden_train_val_rows,
                "frozen_baseline_test_mae": baseline["mae"],
                "frozen_baseline_test_rmse": baseline["rmse"],
                "frozen_baseline_test_rows": baseline["rows"],
                "selected_A_vs_baseline_mae_delta": baseline["mae"] - a_metrics["mae"],
            }
        )

        prediction_frames.append(feature_set_predictions["A"])
        prediction_frames.append(feature_set_predictions["B"])

        test.unpersist()
        train_val.unpersist()
        population.unpersist()

    all_predictions = prediction_frames[0]
    for frame in prediction_frames[1:]:
        all_predictions = all_predictions.unionByName(frame)
    all_predictions = all_predictions.cache()
    total_predictions = all_predictions.count()
    null_predictions = all_predictions.filter(
        F.col("prediction").isNull() | F.isnan("prediction")
    ).count()
    duplicate_prediction_keys = (
        all_predictions.groupBy("location_id", "target_time_utc", "horizon_hours", "feature_set")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    expected_predictions = sum(row["test_rows"] for row in per_horizon_metrics)
    if null_predictions or duplicate_prediction_keys or total_predictions != expected_predictions:
        raise RuntimeError("Gold prediction integrity validation failed")

    (
        all_predictions.repartition(1, "horizon_hours", "feature_set")
        .write.mode("overwrite")
        .partitionBy("horizon_hours", "feature_set")
        .parquet(GOLD_PREDICTIONS_ROOT)
    )

    feature_a_cols = set(FEATURE_CONTRACTS["A"].original_columns)
    feature_b_cols = set(FEATURE_CONTRACTS["B"].original_columns)
    if "pickup_count" in feature_a_cols or "pickup_count" in feature_b_cols:
        raise RuntimeError("Target leakage detected in predictive features")

    report = {
        "report_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "protocol": {
            "input_root": FEATURE_ROOT,
            "physical_month_partitions_read": list(ALL_MONTHS),
            "test_month_partitions": list(TEST_MONTHS),
            "model_family": MODEL_FAMILY,
            "frozen_configurations_source": "data/silver/ml_candidate_selection_report.json (Phase 5C)",
            "frozen_hyperparameters_by_horizon": FROZEN_CONFIGS,
            "refit_split": "train+validation",
            "evaluation_split": "test",
            "preprocessing_fit_split": "train+validation only",
            "nonnegative_prediction_rule": "clip raw prediction at zero before test metrics",
            "no_tuning_or_reselection_after_test": True,
            "gbt_used": False,
        },
        "population_audit": population_audit,
        "preprocessing_audit": preprocessing_audit,
        "test_metrics_by_horizon_and_feature_set": per_horizon_metrics,
        "weather_deltas": weather_deltas,
        "frozen_baseline_test_comparison": {
            str(horizon): {
                "baseline": "previous_week_seasonal_naive",
                **FROZEN_BASELINE_TEST_METRICS[horizon],
                "selected_A_mae": next(
                    row["mae"] for row in per_horizon_metrics
                    if row["horizon_hours"] == horizon and row["feature_set"] == "A"
                ),
            }
            for horizon in HORIZONS
        },
        "gold_outputs": {
            "predictions_uri": GOLD_PREDICTIONS_ROOT,
            "prediction_rows": total_predictions,
            "metrics_uri": GOLD_METRICS_OUTPUT,
        },
        "validation": {
            "test_rows_used_for_fitting_or_preprocessing": 0,
            "target_leakage": False,
            "A_B_test_key_populations_identical": True,
            "unique_prediction_keys": duplicate_prediction_keys == 0,
            "null_or_nan_predictions": null_predictions,
            "nonnegative_rule_identical_for_A_and_B": True,
            "configurations_frozen_before_test_access": True,
            "model_selection_or_reselection_after_test": False,
        },
    }
    write_json_object(spark, GOLD_METRICS_OUTPUT, report)
    local_output = Path("/opt/project/output")
    local_output.mkdir(parents=True, exist_ok=True)
    (local_output / "final_test_evaluation_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        "FINAL_TEST_EVAL_OK "
        + " ".join(
            f"h{row['horizon_hours']}{row['feature_set']}={row['mae']:.6f}"
            for row in per_horizon_metrics
        ),
        flush=True,
    )
    all_predictions.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
