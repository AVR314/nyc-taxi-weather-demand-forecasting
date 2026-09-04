from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime, timedelta

from bronze_ingestion.core import (
    FORECAST_HORIZONS_HOURS,
    NYC_POINTS,
    PUBLICATION_LAG_HOURS,
    TARGET_END,
    TARGET_START,
    WEATHER_VARIABLES,
    floor_to_ecmwf_cycle,
    required_weather_plan,
    summarize_weather_payload,
    weather_object_key,
    weather_request,
)


class WeatherPlanTest(unittest.TestCase):
    def test_required_run_boundaries_and_count(self) -> None:
        plan = required_weather_plan()
        runs = list(plan)
        self.assertEqual(len(runs), 1461)
        self.assertEqual(runs[0], datetime(2024, 12, 31, 12, tzinfo=UTC))
        self.assertEqual(runs[-1], datetime(2025, 12, 31, 12, tzinfo=UTC))
        self.assertEqual(sum(map(len, plan.values())), 8760 * len(FORECAST_HORIZONS_HOURS))

    def test_every_run_is_leakage_safe_and_covers_target(self) -> None:
        for run, requirements in required_weather_plan().items():
            self.assertIn(run.hour, {0, 6, 12, 18})
            for requirement in requirements:
                self.assertLessEqual(
                    run + timedelta(hours=PUBLICATION_LAG_HOURS),
                    requirement["prediction_cutoff"],
                )
                lead_hours = int((requirement["target_time"] - run).total_seconds() / 3600)
                self.assertGreaterEqual(lead_hours, 7)
                self.assertLessEqual(lead_hours, 17)

    def test_cycle_floor_and_request_are_deterministic(self) -> None:
        value = datetime(2025, 2, 3, 17, 59, tzinfo=UTC)
        self.assertEqual(floor_to_ecmwf_cycle(value).hour, 12)
        url, parameters = weather_request(datetime(2025, 1, 1, 6, tzinfo=UTC))
        self.assertIn("single-runs-api.open-meteo.com", url)
        self.assertEqual(parameters["forecast_hours"], "24")
        self.assertEqual(parameters["timezone"], "UTC")
        self.assertEqual(weather_object_key(datetime(2025, 1, 1, 6, tzinfo=UTC)),
                         "bronze/weather/ecmwf_ifs/run=2025-01-01T06-00/response.json")


class WeatherPayloadTest(unittest.TestCase):
    def test_reports_missing_values_without_imputation(self) -> None:
        run = datetime(2025, 1, 1, 0, tzinfo=UTC)
        times = [(run + timedelta(hours=hour)).strftime("%Y-%m-%dT%H:%M") for hour in range(24)]
        payload = []
        for point in NYC_POINTS:
            hourly = {"time": times}
            for variable in WEATHER_VARIABLES:
                hourly[variable] = [1.0] * len(times)
            payload.append(
                {
                    "latitude": point["latitude"],
                    "longitude": point["longitude"],
                    "elevation": 10,
                    "timezone": "GMT",
                    "utc_offset_seconds": 0,
                    "hourly": hourly,
                }
            )
        payload[0]["hourly"]["precipitation"][7] = None
        required = [
            {
                "target_time": run + timedelta(hours=7),
                "horizon_hours": 1,
                "prediction_cutoff": run + timedelta(hours=6),
            }
        ]

        summary = summarize_weather_payload(json.dumps(payload).encode(), run, required)

        self.assertEqual(summary["response_missing_values_by_variable"]["precipitation"], 1)
        self.assertEqual(summary["required_predictor_missing_by_variable"]["precipitation"], 1)
        self.assertEqual(len(summary["missing_required_predictor_slots"]), 1)
        self.assertEqual(summary["missing_required_predictor_slots"][0]["point"], "lower_manhattan")
        self.assertEqual(summary["missing_required_predictor_slots"][0]["horizon_hours"], 1)
        self.assertEqual(summary["missing_required_target_point_horizon_count"], 1)
        self.assertEqual(summary["required_predictor_checks"], len(NYC_POINTS) * len(WEATHER_VARIABLES))

    def test_study_period_has_8760_hours(self) -> None:
        self.assertEqual(int((TARGET_END - TARGET_START).total_seconds() / 3600) + 1, 8760)


if __name__ == "__main__":
    unittest.main()
