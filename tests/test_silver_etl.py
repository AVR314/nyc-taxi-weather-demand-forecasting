from __future__ import annotations

import unittest
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, types as T

from silver_etl.transforms import (
    WEATHER_VARIABLES,
    classify_taxi,
    complete_demand_grid,
    complete_weather_records,
)


class SilverSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("silver-etl-focused-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_dst_anomalies_and_local_year_boundaries(self) -> None:
        taxi = self.spark.createDataFrame(
            [
                (datetime(2025, 1, 1, 0, 0), 1),
                (datetime(2025, 3, 9, 2, 30), 1),
                (datetime(2025, 11, 2, 1, 30), 1),
                (datetime(2024, 12, 31, 23, 59), 1),
                (datetime(2026, 1, 1, 0, 0), 1),
            ],
            "tpep_pickup_datetime timestamp, PULocationID int",
        )
        zones = self.spark.createDataFrame(
            [(1, "Manhattan", "Test", "Yellow Zone")],
            "LocationID int, Borough string, Zone string, service_zone string",
        )
        rows = classify_taxi(taxi, zones).orderBy("tpep_pickup_datetime").collect()
        by_time = {row.tpep_pickup_datetime: row for row in rows}
        self.assertEqual(by_time[datetime(2025, 1, 1)].record_status, "accepted")
        self.assertEqual(by_time[datetime(2025, 1, 1)].pickup_time_utc, datetime(2025, 1, 1, 5))
        self.assertEqual(
            by_time[datetime(2025, 3, 9, 2, 30)].record_reason,
            "dst_nonexistent_spring_forward",
        )
        self.assertEqual(
            by_time[datetime(2025, 11, 2, 1, 30)].record_reason,
            "dst_ambiguous_fall_back",
        )
        self.assertEqual(by_time[datetime(2024, 12, 31, 23, 59)].record_reason, "outside_local_year_2025")
        self.assertEqual(by_time[datetime(2026, 1, 1)].record_reason, "outside_local_year_2025")

    def test_zero_grid_and_dst_unavailable_hours(self) -> None:
        accepted = self.spark.createDataFrame(
            [(1, datetime(2025, 1, 1, 5))],
            "PULocationID int, pickup_hour_utc timestamp",
        )
        zones = self.spark.createDataFrame([(1,), (2,)], "LocationID int")
        hours = self.spark.createDataFrame(
            [
                (datetime(2025, 1, 1, 5),),
                (datetime(2025, 11, 2, 5),),
                (datetime(2025, 11, 2, 6),),
            ],
            "target_time_utc timestamp",
        )
        grid = complete_demand_grid(accepted, zones, hours)
        self.assertEqual(grid.count(), 6)
        normal = grid.filter(F.col("target_time_utc") == datetime(2025, 1, 1, 5)).orderBy(
            "location_id"
        ).collect()
        self.assertEqual([row.pickup_count for row in normal], [1, 0])
        unavailable = grid.filter(~F.col("demand_available")).collect()
        self.assertEqual(len(unavailable), 4)
        self.assertTrue(all(row.pickup_count is None for row in unavailable))

    def weather_frames(self):
        run = datetime(2024, 12, 31, 18)
        target = datetime(2025, 1, 1, 5)
        expected = self.spark.createDataFrame(
            [
                (run, target, 1, datetime(2025, 1, 1, 4), "a"),
                (run, target, 1, datetime(2025, 1, 1, 4), "b"),
            ],
            "run_initialization_utc timestamp, target_time_utc timestamp, horizon_hours int, "
            "prediction_cutoff_utc timestamp, weather_point string",
        )
        fields = [
            T.StructField("run_initialization_utc", T.TimestampType(), False),
            T.StructField("target_time_utc", T.TimestampType(), False),
            T.StructField("weather_point", T.StringType(), False),
            T.StructField("provider_latitude", T.DoubleType(), True),
            T.StructField("provider_longitude", T.DoubleType(), True),
            T.StructField("provider_timezone", T.StringType(), True),
            T.StructField("provider_utc_offset_seconds", T.IntegerType(), True),
        ] + [T.StructField(variable, T.DoubleType(), True) for variable in WEATHER_VARIABLES]
        values = [1.0] * len(WEATHER_VARIABLES)
        values[0] = None
        parsed = self.spark.createDataFrame(
            [(run, target, "a", 40.7, -74.0, "GMT", 0, *values)],
            T.StructType(fields),
        )
        return expected, parsed

    def test_missing_weather_is_preserved(self) -> None:
        expected, parsed = self.weather_frames()
        result = complete_weather_records(expected, parsed).orderBy("weather_point").collect()
        self.assertEqual(len(result), 2)
        self.assertTrue(result[0].source_response_available)
        self.assertTrue(result[0].temperature_2m_missing)
        self.assertTrue(result[0].any_weather_missing)
        self.assertFalse(result[1].source_response_available)
        self.assertTrue(result[1].any_weather_missing)
        self.assertTrue(all(result[1][f"{variable}_missing"] for variable in WEATHER_VARIABLES))

    def test_weather_plan_is_leakage_safe(self) -> None:
        expected, parsed = self.weather_frames()
        result = complete_weather_records(expected, parsed)
        violations = result.filter(
            F.col("run_initialization_utc").cast("long") + 6 * 3600
            > F.col("prediction_cutoff_utc").cast("long")
        ).count()
        self.assertEqual(violations, 0)

    def test_join_cardinality_has_no_many_to_many_expansion(self) -> None:
        demand = self.spark.createDataFrame(
            [(1, datetime(2025, 1, 1, 5), 3, True), (2, datetime(2025, 1, 1, 5), 0, True)],
            "location_id int, target_time_utc timestamp, pickup_count long, demand_available boolean",
        )
        mapping = self.spark.createDataFrame([(1, "a"), (2, "b")], "location_id int, weather_point string")
        weather = self.spark.createDataFrame(
            [
                (datetime(2025, 1, 1, 5), point, horizon)
                for point in ("a", "b")
                for horizon in (1, 3, 6)
            ],
            "target_time_utc timestamp, weather_point string, horizon_hours int",
        )
        joined = demand.join(mapping, "location_id").join(
            weather, ["target_time_utc", "weather_point"], "left"
        )
        self.assertEqual(joined.count(), demand.count() * 3)
        duplicates = joined.groupBy("location_id", "target_time_utc", "horizon_hours").count().filter(
            F.col("count") > 1
        )
        self.assertEqual(duplicates.count(), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
