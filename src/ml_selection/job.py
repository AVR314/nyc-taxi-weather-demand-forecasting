"""Select forecasting ML candidates using train and validation only."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import DataFrame, SparkSession, functions as F

from forecast_baselines.transforms import assign_chronological_split
from ml_selection.transforms import (
    COMMON_CATEGORICAL_COLUMNS,
    FEATURE_CONTRACTS,
    FEATURE_ROOT,
    GBT_GRID,
    LINEAR_GRID,
    MODEL_SEED,
    ROW_KEYS,
    WEATHER_CATEGORICAL_COLUMNS,
    WEATHER_NUMERIC_COLUMNS,
    best_validation_result,
    categorical_vocabularies,
    clip_nonnegative,
    feature_partition_paths,
    prepare_model_columns,
    preprocessing_pipeline,
    validation_metric_aggregates,
)


REPORT_OUTPUT = "s3a://bigdata/silver/manifests/ml_candidate_selection_report.json"
HORIZONS = (1, 3, 6)
MODEL_FAMILIES = ("regularized_linear_regression", "gradient_boosted_trees")
VALIDATION_END_LOCAL_EXCLUSIVE = "2025-11-01 00:00:00"


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("nyc-taxi-weather-ml-candidate-selection")
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


def read_train_validation_horizon(spark: SparkSession, horizon: int) -> DataFrame:
    paths = feature_partition_paths(horizon)
    if any("target_local_month=11" in path or "target_local_month=12" in path for path in paths):
        raise RuntimeError("Frozen test partitions must never be read")
    return (
        spark.read.option("basePath", FEATURE_ROOT)
        .parquet(*paths)
        .transform(assign_chronological_split)
        .filter(F.col("split").isin("train", "validation"))
        .filter(F.col("paired_evaluation_eligible"))
    )


def build_estimator(family: str, parameters: dict):
    if family == "regularized_linear_regression":
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
    if family == "gradient_boosted_trees":
        return GBTRegressor(
            featuresCol="features",
            labelCol="pickup_count",
            predictionCol="raw_prediction",
            maxDepth=parameters["max_depth"],
            maxIter=parameters["max_iter"],
            stepSize=parameters["step_size"],
            lossType="squared",
            subsamplingRate=1.0,
            featureSubsetStrategy="all",
            seed=MODEL_SEED,
        )
    raise ValueError(f"Unsupported model family: {family}")


def fit_and_score_grid(
    family: str,
    feature_set: str,
    horizon: int,
    train: DataFrame,
    validation: DataFrame,
    train_rows: int,
    validation_rows: int,
) -> list[dict]:
    grid = LINEAR_GRID if family == "regularized_linear_regression" else GBT_GRID
    results = []
    for index, parameters in enumerate(grid, start=1):
        model = build_estimator(family, parameters).fit(train)
        scored = clip_nonnegative(model.transform(validation)).select(
            "pickup_count", "raw_prediction", "prediction"
        )
        metrics = scored.agg(*validation_metric_aggregates()).first()
        result = {
            "model_family": family,
            "feature_set": feature_set,
            "horizon_hours": horizon,
            "configuration_id": index,
            "hyperparameters": dict(parameters),
            "seed": MODEL_SEED if family == "gradient_boosted_trees" else None,
            "train_rows": train_rows,
            "validation_rows": validation_rows,
            "prediction_rows": int(metrics["prediction_rows"]),
            "raw_negative_prediction_rows": int(metrics["raw_negative_prediction_rows"]),
            "invalid_prediction_rows": int(metrics["invalid_prediction_rows"]),
            "nonnegative_rule": "clip raw prediction at zero before metrics",
            "mae": float(metrics["mae"]),
            "rmse": float(metrics["rmse"]),
        }
        if result["prediction_rows"] != validation_rows or result["invalid_prediction_rows"]:
            raise RuntimeError("Validation prediction cardinality or validity failed")
        results.append(result)
    return results


def baseline_metrics(validation: DataFrame, horizon: int) -> dict:
    scored = validation.withColumn(
        "prediction", F.col("demand_same_local_hour_previous_week").cast("double")
    ).withColumn("raw_prediction", F.col("prediction"))
    metrics = scored.agg(*validation_metric_aggregates()).first()
    return {
        "baseline": "previous_week_seasonal_naive",
        "horizon_hours": horizon,
        "validation_rows": int(metrics["prediction_rows"]),
        "mae": float(metrics["mae"]),
        "rmse": float(metrics["rmse"]),
    }


def main() -> None:
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    all_grid_results: list[dict] = []
    best_results: list[dict] = []
    baseline_results: list[dict] = []
    preprocessing_audit: list[dict] = []
    population_audit: list[dict] = []

    for horizon in HORIZONS:
        population = read_train_validation_horizon(spark, horizon).cache()
        population_rows = population.count()
        forbidden_rows = population.filter(
            F.col("split_target_local_time")
            >= F.to_timestamp(F.lit(VALIDATION_END_LOCAL_EXCLUSIVE))
        ).count()
        duplicate_keys = duplicate_count(population)
        train_source = population.filter(F.col("split") == "train").cache()
        validation_source = population.filter(F.col("split") == "validation").cache()
        train_rows = train_source.count()
        validation_rows = validation_source.count()
        if forbidden_rows or duplicate_keys or population_rows != train_rows + validation_rows:
            raise RuntimeError("Train/validation population validation failed")

        baseline_results.append(baseline_metrics(validation_source, horizon))
        shared_vocabularies: dict[str, dict[str, list[str]]] = {}

        for feature_set, contract in FEATURE_CONTRACTS.items():
            prepared_train = prepare_model_columns(train_source, contract)
            prepared_validation = prepare_model_columns(validation_source, contract)
            preprocessor = preprocessing_pipeline(contract).fit(prepared_train)
            vocabularies = categorical_vocabularies(preprocessor, contract)
            shared_vocabularies[feature_set] = vocabularies
            transformed_train = preprocessor.transform(prepared_train).select(
                *ROW_KEYS, "pickup_count", "features"
            ).cache()
            transformed_validation = preprocessor.transform(prepared_validation).select(
                *ROW_KEYS, "pickup_count", "features"
            ).cache()
            transformed_train_rows = transformed_train.count()
            transformed_validation_rows = transformed_validation.count()
            if transformed_train_rows != train_rows or transformed_validation_rows != validation_rows:
                raise RuntimeError("Preprocessing changed the A/B population")

            preprocessing_audit.append(
                {
                    "horizon_hours": horizon,
                    "feature_set": feature_set,
                    "fit_split": "train",
                    "fit_rows": train_rows,
                    "validation_transform_rows": validation_rows,
                    "categorical_encoding": "StringIndexer(handleInvalid=keep) then OneHotEncoder(dropLast=false, handleInvalid=keep)",
                    "categorical_columns": list(contract.categorical_columns),
                    "numeric_columns": list(contract.numeric_columns),
                    "categorical_vocabularies": vocabularies,
                }
            )

            for family in MODEL_FAMILIES:
                grid_results = fit_and_score_grid(
                    family,
                    feature_set,
                    horizon,
                    transformed_train,
                    transformed_validation,
                    train_rows,
                    validation_rows,
                )
                all_grid_results.extend(grid_results)
                best_results.append(best_validation_result(grid_results))

            transformed_validation.unpersist()
            transformed_train.unpersist()

        for column in COMMON_CATEGORICAL_COLUMNS:
            if shared_vocabularies["A"][column] != shared_vocabularies["B"][column]:
                raise RuntimeError(f"A/B categorical vocabulary mismatch: {column}")
        population_audit.append(
            {
                "horizon_hours": horizon,
                "physical_month_partitions_read": list(range(1, 11)),
                "test_month_partitions_read": [],
                "train_rows_feature_set_A": train_rows,
                "train_rows_feature_set_B": train_rows,
                "validation_rows_feature_set_A": validation_rows,
                "validation_rows_feature_set_B": validation_rows,
                "A_B_key_symmetric_difference": 0,
                "duplicate_row_keys": duplicate_keys,
                "forbidden_test_rows": forbidden_rows,
            }
        )
        validation_source.unpersist()
        train_source.unpersist()
        population.unpersist()

    baseline_by_horizon = {row["horizon_hours"]: row for row in baseline_results}
    for result in best_results:
        baseline = baseline_by_horizon[result["horizon_hours"]]
        result["baseline_mae"] = baseline["mae"]
        result["baseline_rmse"] = baseline["rmse"]
        result["mae_improvement_over_baseline"] = baseline["mae"] - result["mae"]
        result["mae_improvement_over_baseline_percent"] = (
            100.0 * (baseline["mae"] - result["mae"]) / baseline["mae"]
        )

    weather_deltas = []
    for horizon in HORIZONS:
        for family in MODEL_FAMILIES:
            result_a = next(
                row
                for row in best_results
                if row["horizon_hours"] == horizon
                and row["model_family"] == family
                and row["feature_set"] == "A"
            )
            result_b = next(
                row
                for row in best_results
                if row["horizon_hours"] == horizon
                and row["model_family"] == family
                and row["feature_set"] == "B"
            )
            weather_deltas.append(
                {
                    "horizon_hours": horizon,
                    "model_family": family,
                    "A_configuration_id": result_a["configuration_id"],
                    "B_configuration_id": result_b["configuration_id"],
                    "A_mae": result_a["mae"],
                    "B_mae": result_b["mae"],
                    "mae_weather_improvement": result_a["mae"] - result_b["mae"],
                    "mae_weather_improvement_percent": 100.0
                    * (result_a["mae"] - result_b["mae"])
                    / result_a["mae"],
                    "A_rmse": result_a["rmse"],
                    "B_rmse": result_b["rmse"],
                    "rmse_weather_improvement": result_a["rmse"] - result_b["rmse"],
                    "rmse_weather_improvement_percent": 100.0
                    * (result_a["rmse"] - result_b["rmse"])
                    / result_a["rmse"],
                }
            )

    selected_by_horizon = {
        str(horizon): best_validation_result(
            [row for row in best_results if row["horizon_hours"] == horizon]
        )
        for horizon in HORIZONS
    }
    feature_a = set(FEATURE_CONTRACTS["A"].original_columns)
    feature_b = set(FEATURE_CONTRACTS["B"].original_columns)
    if feature_b - feature_a != set(WEATHER_CATEGORICAL_COLUMNS + WEATHER_NUMERIC_COLUMNS):
        raise RuntimeError("Feature set B differs from A by non-weather information")
    if "pickup_count" in feature_a or "pickup_count" in feature_b:
        raise RuntimeError("Target leakage detected in predictive features")

    report = {
        "report_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "protocol": {
            "input_root": FEATURE_ROOT,
            "physical_month_partitions_read": list(range(1, 11)),
            "physical_test_month_partitions_read": [],
            "splits_used": ["train", "validation"],
            "test_read_scored_or_inspected": False,
            "primary_population": "paired_evaluation_eligible == true",
            "horizons_trained_separately": list(HORIZONS),
            "selection_metric": "validation MAE",
            "secondary_metric": "validation RMSE",
            "preprocessing_fit_split": "train only",
            "nonnegative_prediction_rule": "clip raw prediction at zero before validation metrics",
            "refit_on_train_plus_validation": False,
            "imputation": False,
        },
        "feature_contracts": {
            name: {
                "categorical_columns": list(contract.categorical_columns),
                "numeric_columns": list(contract.numeric_columns),
            }
            for name, contract in FEATURE_CONTRACTS.items()
        },
        "predeclared_grids": {
            "regularized_linear_regression": list(LINEAR_GRID),
            "gradient_boosted_trees": list(GBT_GRID),
            "gradient_boosted_trees_seed": MODEL_SEED,
        },
        "population_audit": population_audit,
        "preprocessing_audit": preprocessing_audit,
        "validation_baseline": baseline_results,
        "all_grid_validation_results": all_grid_results,
        "best_by_family_horizon_feature_set": best_results,
        "weather_deltas": weather_deltas,
        "selected_by_horizon": selected_by_horizon,
        "validation": {
            "target_leakage": False,
            "test_rows_used": 0,
            "preprocessing_fit_on_validation": False,
            "A_B_shared_categorical_vocabularies_match": True,
            "A_B_feature_difference_is_weather_only": True,
            "A_B_population_key_difference": 0,
            "prediction_count_mismatches": 0,
            "nonnegative_rule_identical_for_all_models": True,
        },
    }
    write_json_object(spark, REPORT_OUTPUT, report)
    local_output = Path("/opt/project/output")
    local_output.mkdir(parents=True, exist_ok=True)
    (local_output / "ml_candidate_selection_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        "ML_SELECTION_OK "
        + " ".join(
            f"h{horizon}={selected_by_horizon[str(horizon)]['model_family']}:"
            f"{selected_by_horizon[str(horizon)]['feature_set']}:"
            f"{selected_by_horizon[str(horizon)]['mae']:.6f}"
            for horizon in HORIZONS
        ),
        flush=True,
    )
    spark.stop()


if __name__ == "__main__":
    main()
