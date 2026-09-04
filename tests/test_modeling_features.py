from __future__ import annotations

import math
import unittest
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F, types as T

from modeling_features.transforms import (
    DEMAND_FEATURE_COLUMNS,
    FEATURE_SET_A_COLUMNS,
    FEATURE_SET_B_COLUMNS,
    WEATHER_FEATURE_COLUMNS,
    WEATHER_VARIABLES,
    build_modeling_features,
)


class ModelingFeatureSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("modeling-feature-focused-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        start = datetime(2025, 1, 1, 5)
        history_rows = []
        for zone, offset in ((1, 0), (2, 1000)):
            for index in range(200):
                history_rows.append((zone, start + timedelta(hours=index), True, offset + index))
        cls.history = cls.spark.createDataFrame(
            history_rows,
            "location_id int, target_time_utc timestamp, demand_available boolean, pickup_count long",
        )
        target = start + timedelta(hours=198)
        candidates = cls.candidate_frame([(1, target, 3, 999, False), (2, target, 3, 999, True)])
        cls.features = build_modeling_features(candidates, cls.history).cache()
        cls.features.count()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.features.unpersist()
        cls.spark.stop()

    @classmethod
    def candidate_frame(cls, rows):
        fields = [
            T.StructField("location_id", T.IntegerType(), False),
            T.StructField("target_time_utc", T.TimestampType(), False),
            T.StructField("horizon_hours", T.IntegerType(), False),
            T.StructField("pickup_count", T.LongType(), True),
            T.StructField("any_weather_missing", T.BooleanType(), False),
            T.StructField("source_response_available", T.BooleanType(), False),
        ]
        for variable in WEATHER_VARIABLES:
            fields.append(T.StructField(variable, T.DoubleType(), True))
            fields.append(T.StructField(f"{variable}_missing", T.BooleanType(), False))
        expanded = []
        for zone, target, horizon, target_demand, weather_missing in rows:
            values = [zone, target, horizon, target_demand, weather_missing, True]
            for _ in WEATHER_VARIABLES:
                values.extend([None if weather_missing else 1.0, weather_missing])
            expanded.append(tuple(values))
        return cls.spark.createDataFrame(expanded, T.StructType(fields))

    def test_all_demand_sources_are_at_or_before_cutoff(self) -> None:
        source_columns = (
            "demand_at_cutoff_source_time_utc",
            "demand_cutoff_minus_1h_source_time_utc",
            "demand_cutoff_minus_2h_source_time_utc",
            "previous_day_source_time_utc",
            "previous_week_source_time_utc",
        )
        for column in source_columns:
            self.assertEqual(
                self.features.filter(
                    F.col(column).isNotNull() & (F.col(column) > F.col("prediction_cutoff_utc"))
                ).count(),
                0,
            )
        row = self.features.filter(F.col("location_id") == 1).first()
        self.assertEqual(row.demand_at_cutoff, 195)
        self.assertEqual(row.demand_cutoff_minus_1h, 194)
        self.assertEqual(row.demand_cutoff_minus_2h, 193)

    def test_rolling_windows_end_at_cutoff(self) -> None:
        row = self.features.filter(F.col("location_id") == 1).first()
        self.assertEqual(row.rolling_window_end_utc, row.prediction_cutoff_utc)
        self.assertEqual(row.trailing_3h_start_utc, row.prediction_cutoff_utc - timedelta(hours=2))
        self.assertEqual(row.trailing_6h_start_utc, row.prediction_cutoff_utc - timedelta(hours=5))
        self.assertEqual(row.trailing_24h_start_utc, row.prediction_cutoff_utc - timedelta(hours=23))
        self.assertAlmostEqual(row.demand_trailing_mean_3h, 194.0)
        self.assertAlmostEqual(row.demand_trailing_mean_6h, 192.5)
        self.assertAlmostEqual(row.demand_trailing_mean_24h, 183.5)
        self.assertAlmostEqual(row.demand_trailing_stddev_24h, math.sqrt(47.9166666667), places=6)

    def test_local_day_week_lags_are_zone_specific_and_nonleaking(self) -> None:
        rows = {row.location_id: row for row in self.features.collect()}
        self.assertEqual(rows[1].demand_same_local_hour_previous_day, 174)
        self.assertEqual(rows[2].demand_same_local_hour_previous_day, 1174)
        self.assertEqual(rows[1].demand_same_local_hour_previous_week, 30)
        self.assertLessEqual(rows[1].previous_day_source_time_utc, rows[1].prediction_cutoff_utc)

    def test_feature_sets_differ_only_by_weather(self) -> None:
        self.assertEqual(
            set(FEATURE_SET_B_COLUMNS).difference(FEATURE_SET_A_COLUMNS),
            set(WEATHER_FEATURE_COLUMNS),
        )
        self.assertNotIn("pickup_count", FEATURE_SET_A_COLUMNS)
        self.assertNotIn("pickup_count", FEATURE_SET_B_COLUMNS)
        rows = {row.location_id: row for row in self.features.collect()}
        self.assertFalse(rows[1].any_weather_missing)
        self.assertTrue(rows[2].any_weather_missing)
        self.assertTrue(rows[1].paired_evaluation_eligible)
        self.assertFalse(rows[2].paired_evaluation_eligible)

    def test_dst_local_calendar_fields_remain_correct(self) -> None:
        candidates = self.candidate_frame(
            [
                (1, datetime(2025, 3, 9, 6), 1, 1, False),
                (1, datetime(2025, 3, 9, 7), 1, 1, False),
                (1, datetime(2025, 11, 2, 5), 1, None, False),
                (1, datetime(2025, 11, 2, 6), 1, None, False),
            ]
        )
        rows = build_modeling_features(candidates, self.history).orderBy("target_time_utc").collect()
        self.assertEqual([row.target_local_hour for row in rows], [1, 3, 1, 1])
        self.assertNotEqual(rows[2].target_time_utc, rows[3].target_time_utc)

    def test_row_keys_are_unique(self) -> None:
        duplicates = self.features.groupBy(
            "location_id", "target_time_utc", "horizon_hours"
        ).count().filter(F.col("count") > 1)
        self.assertEqual(duplicates.count(), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
