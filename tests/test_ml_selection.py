from __future__ import annotations

import unittest
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from ml_selection.transforms import (
    COMMON_CATEGORICAL_COLUMNS,
    FEATURE_CONTRACTS,
    ROW_KEYS,
    best_validation_result,
    categorical_vocabularies,
    clip_nonnegative,
    feature_partition_paths,
    prepare_model_columns,
    preprocessing_pipeline,
    validation_metric_aggregates,
)


class MLSelectionSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("ml-selection-focused-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def categorical_frame(self, locations: list[int], months: list[int]):
        rows = [
            (location, hour % 24, (hour % 7) + 1, month, float(hour), hour % 2 == 0)
            for hour, (location, month) in enumerate(zip(locations, months))
        ]
        return self.spark.createDataFrame(
            rows,
            "location_id int, target_local_hour int, target_local_day_of_week int, "
            "target_local_month int, demand_at_cutoff double, target_local_is_weekend boolean",
        )

    def test_preprocessing_is_fit_on_train_only_and_handles_unseen_categories(self) -> None:
        contract = FEATURE_CONTRACTS["A"]
        train = self.categorical_frame([1, 1, 2], [1, 1, 2])
        validation = self.categorical_frame([3], [9])
        for column in contract.numeric_columns:
            if column not in train.columns:
                train = train.withColumn(column, F.lit(1.0))
                validation = validation.withColumn(column, F.lit(1.0))
        prepared_train = prepare_model_columns(train, contract)
        prepared_validation = prepare_model_columns(validation, contract)
        model = preprocessing_pipeline(contract).fit(prepared_train)
        vocabularies = categorical_vocabularies(model, contract)
        self.assertNotIn("3", vocabularies["location_id"])
        self.assertNotIn("9", vocabularies["target_local_month"])
        self.assertEqual(model.transform(prepared_validation).count(), 1)

    def test_location_id_is_categorical_in_both_feature_sets(self) -> None:
        for contract in FEATURE_CONTRACTS.values():
            self.assertIn("location_id", contract.categorical_columns)
            self.assertNotIn("location_id", contract.numeric_columns)

    def test_A_B_feature_parity_differs_only_by_weather(self) -> None:
        a = set(FEATURE_CONTRACTS["A"].original_columns)
        b = set(FEATURE_CONTRACTS["B"].original_columns)
        difference = b - a
        self.assertTrue(difference)
        self.assertTrue(all(column not in COMMON_CATEGORICAL_COLUMNS for column in difference))
        self.assertEqual(a - b, set())
        self.assertNotIn("pickup_count", a | b)

    def test_no_test_partition_access(self) -> None:
        for horizon in (1, 3, 6):
            paths = feature_partition_paths(horizon)
            self.assertEqual(len(paths), 10)
            self.assertFalse(any("target_local_month=11" in path for path in paths))
            self.assertFalse(any("target_local_month=12" in path for path in paths))

    def test_metric_correctness_after_nonnegative_clipping(self) -> None:
        frame = self.spark.createDataFrame(
            [(2.0, -1.0), (4.0, 5.0)], "pickup_count double, raw_prediction double"
        )
        metrics = clip_nonnegative(frame).agg(*validation_metric_aggregates()).first()
        self.assertEqual(metrics.prediction_rows, 2)
        self.assertEqual(metrics.raw_negative_prediction_rows, 1)
        self.assertAlmostEqual(metrics.mae, 1.5)
        self.assertAlmostEqual(metrics.rmse, (2.5) ** 0.5)

    def test_nonnegative_policy_is_identical_and_deterministic(self) -> None:
        frame = self.spark.createDataFrame(
            [(-2.0,), (0.5,)], "raw_prediction double"
        )
        self.assertEqual(
            [row.prediction for row in clip_nonnegative(frame).collect()], [0.0, 0.5]
        )

    def test_validation_selection_uses_mae_then_rmse(self) -> None:
        results = [
            {"model_family": "a", "feature_set": "A", "configuration_id": 1, "mae": 2.0, "rmse": 3.0},
            {"model_family": "b", "feature_set": "B", "configuration_id": 1, "mae": 1.0, "rmse": 4.0},
            {"model_family": "c", "feature_set": "A", "configuration_id": 1, "mae": 1.0, "rmse": 2.0},
        ]
        self.assertEqual(best_validation_result(results)["model_family"], "c")

    def test_unique_row_key_definition(self) -> None:
        self.assertEqual(ROW_KEYS, ("location_id", "target_time_utc", "horizon_hours"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
