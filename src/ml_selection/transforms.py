"""Feature contracts and train-only preprocessing for Phase 5C."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, functions as F

from modeling_features.transforms import (
    DEMAND_FEATURE_COLUMNS,
    WEATHER_FEATURE_COLUMNS,
)


FEATURE_ROOT = "s3a://bigdata/silver/modeling_features/records"
TRAIN_VALIDATION_MONTHS = tuple(range(1, 11))
ROW_KEYS = ("location_id", "target_time_utc", "horizon_hours")
COMMON_CATEGORICAL_COLUMNS = (
    "location_id",
    "target_local_hour",
    "target_local_day_of_week",
    "target_local_month",
)
COMMON_NUMERIC_COLUMNS = DEMAND_FEATURE_COLUMNS + ("target_local_is_weekend",)
WEATHER_CATEGORICAL_COLUMNS = ("weather_code",)
WEATHER_NUMERIC_COLUMNS = tuple(
    column for column in WEATHER_FEATURE_COLUMNS if column != "weather_code"
)
LINEAR_GRID = (
    {"reg_param": 0.01, "elastic_net_param": 0.0, "max_iter": 50},
    {"reg_param": 0.1, "elastic_net_param": 0.0, "max_iter": 50},
    {"reg_param": 1.0, "elastic_net_param": 0.0, "max_iter": 50},
)
GBT_GRID = (
    {"max_depth": 4, "max_iter": 15, "step_size": 0.1},
    {"max_depth": 5, "max_iter": 25, "step_size": 0.05},
)
MODEL_SEED = 314159


@dataclass(frozen=True)
class FeatureContract:
    name: str
    categorical_columns: tuple[str, ...]
    numeric_columns: tuple[str, ...]

    @property
    def original_columns(self) -> tuple[str, ...]:
        return self.categorical_columns + self.numeric_columns


FEATURE_CONTRACTS = {
    "A": FeatureContract(
        name="A",
        categorical_columns=COMMON_CATEGORICAL_COLUMNS,
        numeric_columns=COMMON_NUMERIC_COLUMNS,
    ),
    "B": FeatureContract(
        name="B",
        categorical_columns=COMMON_CATEGORICAL_COLUMNS + WEATHER_CATEGORICAL_COLUMNS,
        numeric_columns=COMMON_NUMERIC_COLUMNS + WEATHER_NUMERIC_COLUMNS,
    ),
}


def feature_partition_paths(horizon: int) -> list[str]:
    if horizon not in (1, 3, 6):
        raise ValueError(f"Unsupported horizon: {horizon}")
    return [
        f"{FEATURE_ROOT}/horizon_hours={horizon}/target_local_month={month}"
        for month in TRAIN_VALIDATION_MONTHS
    ]


def prepare_model_columns(frame: DataFrame, contract: FeatureContract) -> DataFrame:
    result = frame
    for column in contract.categorical_columns:
        result = result.withColumn(f"model_cat__{column}", F.col(column).cast("string"))
    for column in contract.numeric_columns:
        result = result.withColumn(f"model_num__{column}", F.col(column).cast("double"))
    return result


def preprocessing_pipeline(contract: FeatureContract) -> Pipeline:
    index_columns = [f"model_idx__{column}" for column in contract.categorical_columns]
    vector_columns = [f"model_ohe__{column}" for column in contract.categorical_columns]
    indexers = [
        StringIndexer(
            inputCol=f"model_cat__{column}",
            outputCol=index_column,
            handleInvalid="keep",
            stringOrderType="frequencyDesc",
        )
        for column, index_column in zip(contract.categorical_columns, index_columns)
    ]
    encoder = OneHotEncoder(
        inputCols=index_columns,
        outputCols=vector_columns,
        handleInvalid="keep",
        dropLast=False,
    )
    assembler = VectorAssembler(
        inputCols=vector_columns
        + [f"model_num__{column}" for column in contract.numeric_columns],
        outputCol="features",
        handleInvalid="error",
    )
    return Pipeline(stages=[*indexers, encoder, assembler])


def categorical_vocabularies(pipeline_model, contract: FeatureContract) -> dict[str, list[str]]:
    return {
        column: list(pipeline_model.stages[index].labels)
        for index, column in enumerate(contract.categorical_columns)
    }


def clip_nonnegative(frame: DataFrame, raw_column: str = "raw_prediction") -> DataFrame:
    return frame.withColumn("prediction", F.greatest(F.col(raw_column), F.lit(0.0)))


def validation_metric_aggregates() -> list:
    absolute_error = F.abs(F.col("prediction") - F.col("pickup_count"))
    squared_error = F.pow(F.col("prediction") - F.col("pickup_count"), 2)
    return [
        F.count(F.lit(1)).alias("prediction_rows"),
        F.sum((F.col("raw_prediction") < 0).cast("long")).alias(
            "raw_negative_prediction_rows"
        ),
        F.sum(
            (
                F.col("raw_prediction").isNull()
                | F.isnan("raw_prediction")
                | F.col("prediction").isNull()
                | F.isnan("prediction")
            ).cast("long")
        ).alias("invalid_prediction_rows"),
        F.avg(absolute_error).alias("mae"),
        F.sqrt(F.avg(squared_error)).alias("rmse"),
    ]


def best_validation_result(results: list[dict]) -> dict:
    if not results:
        raise ValueError("Validation results are required")
    return min(
        results,
        key=lambda row: (
            row["mae"],
            row["rmse"],
            row["model_family"],
            row["feature_set"],
            row["configuration_id"],
        ),
    )
