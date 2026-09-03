from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.profile_taxi_data import profile_file


class TaxiProfileTest(unittest.TestCase):
    def test_quality_counts_and_zone_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            parquet_path = root / "sample.parquet"
            lookup_path = root / "zones.csv"
            with lookup_path.open("w", encoding="utf-8", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=["LocationID", "Borough", "Zone", "service_zone"])
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "LocationID": 1,
                            "Borough": "Manhattan",
                            "Zone": "One",
                            "service_zone": "Yellow Zone",
                        },
                        {
                            "LocationID": 2,
                            "Borough": "Queens",
                            "Zone": "Two",
                            "service_zone": "Boro Zone",
                        },
                    ]
                )

            pickup = [
                datetime(2025, 1, 1, 1),
                datetime(2025, 1, 1, 2),
                datetime(2024, 12, 31, 23),
                datetime(2025, 1, 2, 5),
            ]
            dropoff = [
                datetime(2025, 1, 1, 1, 10),
                datetime(2025, 1, 1, 2),
                datetime(2025, 1, 1, 0, 15),
                datetime(2025, 1, 3, 6),
            ]
            table = pa.table(
                {
                    "VendorID": [1, 1, 2, 2],
                    "tpep_pickup_datetime": pickup,
                    "tpep_dropoff_datetime": dropoff,
                    "passenger_count": [1, 0, 2, None],
                    "trip_distance": [1.0, 0.0, -1.0, 150.0],
                    "RatecodeID": [1, 1, 1, None],
                    "PULocationID": [1, 2, 999, None],
                    "DOLocationID": [2, 1, 999, 2],
                    "payment_type": [1, 2, 1, None],
                    "fare_amount": [10.0, 0.0, -5.0, 600.0],
                    "total_amount": [12.0, 0.0, -3.0, 1200.0],
                }
            )
            pq.write_table(table, parquet_path)

            profile = profile_file(
                parquet_path,
                lookup_path,
                "2025-01",
                "https://example.test/sample.parquet",
                "https://example.test/zones.csv",
            )

            self.assertEqual(profile["parquet"]["rows"], 4)
            self.assertEqual(profile["timestamps"]["tpep_pickup_datetime"]["outside_expected_month"], 1)
            self.assertEqual(profile["location_ids"]["PULocationID"]["distinct_non_null"], 3)
            self.assertEqual(profile["location_ids"]["PULocationID"]["not_in_official_lookup"], 1)
            self.assertEqual(profile["location_ids"]["PULocationID"]["outside_lookup_id_range"], 1)
            self.assertEqual(profile["quality_flags"]["trip_distance_zero"], 1)
            self.assertEqual(profile["quality_flags"]["trip_distance_negative"], 1)
            self.assertEqual(profile["quality_flags"]["fare_amount_over_500"], 1)
            self.assertEqual(profile["quality_flags"]["duration_zero"], 1)
            self.assertEqual(profile["quality_flags"]["duration_over_24_hours"], 1)


if __name__ == "__main__":
    unittest.main()
