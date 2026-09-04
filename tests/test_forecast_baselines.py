from __future__ import annotations

import unittest
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from forecast_baselines.transforms import (
    BASELINES,
    ROW_KEYS,
    assign_chronological_split,
    baseline_predictions_long,
    exclusion_aggregates,
    metric_aggregates,
    paired_population,
    select_strongest_baseline,
)


class ForecastBaselineSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("forecast-baseline-focused-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def feature_frame(self):
        rows = [
            (1, datetime(2025, 8, 31, 3), 1, 10, 8, 7, 6, datetime(2025, 8, 31, 2), True, False, True),
            (1, datetime(2025, 9, 1, 4), 1, 12, 10, 9, 8, datetime(2025, 9, 1, 3), True, False, True),
            (1, datetime(2025, 11, 1, 4), 1, 14, 11, 10, 9, datetime(2025, 11, 1, 3), True, False, True),
            (2, datetime(2025, 11, 1, 4), 3, 20, None, 18, 17, datetime(2025, 11, 1, 1), False, False, False),
            (2, datetime(2025, 11, 1, 5), 3, 21, 19, 18, 17, datetime(2025, 11, 1, 2), True, True, False),
        ]
        schema = (
            "location_id int, target_time_utc timestamp, horizon_hours int, pickup_count long, "
            "demand_at_cutoff long, demand_same_local_hour_previous_day long, "
            "demand_same_local_hour_previous_week long, prediction_cutoff_utc timestamp, "
            "demand_history_complete boolean, any_weather_missing boolean, "
            "paired_evaluation_eligible boolean"
        )
        frame = self.spark.createDataFrame(rows, schema)
        return (
            frame.withColumn("demand_at_cutoff_source_time_utc", F.col("prediction_cutoff_utc"))
            .withColumn("previous_day_source_time_utc", F.col("prediction_cutoff_utc"))
            .withColumn("previous_week_source_time_utc", F.col("prediction_cutoff_utc"))
        )

    def test_half_open_local_split_boundaries(self) -> None:
        rows = [
            (datetime(2025, 9, 1, 3, 59),),
            (datetime(2025, 9, 1, 4, 0),),
            (datetime(2025, 11, 1, 3, 59),),
            (datetime(2025, 11, 1, 4, 0),),
            (datetime(2026, 1, 1, 4, 59),),
            (datetime(2026, 1, 1, 5, 0),),
        ]
        frame = self.spark.createDataFrame(rows, "target_time_utc timestamp")
        splits = [row.split for row in assign_chronological_split(frame).collect()]
        self.assertEqual(splits, ["train", "validation", "validation", "test", "test", None])

    def test_primary_population_and_exclusion_reasons(self) -> None:
        assigned = assign_chronological_split(self.feature_frame())
        self.assertEqual(paired_population(assigned).count(), 3)
        test = assigned.filter(F.col("split") == "test").agg(*exclusion_aggregates()).first()
        self.assertEqual(test.candidate_rows, 3)
        self.assertEqual(test.paired_rows, 1)
        self.assertEqual(test.demand_history_only_rows, 1)
        self.assertEqual(test.weather_only_rows, 1)
        self.assertEqual(test.both_missing_rows, 0)

    def test_baseline_predictions_and_metrics(self) -> None:
        assigned = paired_population(assign_chronological_split(self.feature_frame()))
        long = baseline_predictions_long(assigned)
        self.assertEqual(long.count(), assigned.count() * len(BASELINES))
        persistence = long.filter(F.col("baseline") == "persistence")
        metric = persistence.agg(*metric_aggregates()).first()
        self.assertEqual(metric.rows, 3)
        self.assertAlmostEqual(metric.mae, (2 + 2 + 3) / 3)

    def test_baseline_sources_do_not_exceed_cutoff(self) -> None:
        predictions = baseline_predictions_long(
            paired_population(assign_chronological_split(self.feature_frame()))
        )
        violations = predictions.filter(
            F.col("prediction_source_time_utc") > F.col("prediction_cutoff_utc")
        ).count()
        self.assertEqual(violations, 0)

    def test_row_keys_and_split_populations_are_unique(self) -> None:
        assigned = assign_chronological_split(self.feature_frame())
        duplicates = assigned.groupBy(*ROW_KEYS).count().filter(F.col("count") > 1).count()
        self.assertEqual(duplicates, 0)
        split_counts = assigned.groupBy("target_time_utc").agg(
            F.countDistinct("split").alias("split_count")
        )
        self.assertEqual(split_counts.filter(F.col("split_count") > 1).count(), 0)

    def test_selection_uses_validation_mae_only(self) -> None:
        validation = [
            {"baseline": "persistence", "mae": 5.0},
            {"baseline": "previous_day_seasonal_naive", "mae": 4.0},
        ]
        selected = select_strongest_baseline(validation)
        self.assertEqual(selected["baseline"], "previous_day_seasonal_naive")
        self.assertNotIn("test", selected)

    def test_target_is_not_a_baseline_predictor(self) -> None:
        self.assertTrue(all(columns[0] != "pickup_count" for columns in BASELINES.values()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
