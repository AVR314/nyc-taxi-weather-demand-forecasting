"""Validate candidate hourly weather APIs and measure NYC spatial variation."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


OPEN_METEO_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "snowfall",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
]

NYC_POINTS = {
    "Lower Manhattan": (40.7128, -74.0060),
    "Bronx": (40.8448, -73.8648),
    "Brooklyn": (40.6782, -73.9442),
    "JFK / Queens": (40.6413, -73.7781),
    "Staten Island": (40.5795, -74.1502),
}


def fetch_json(base_url: str, parameters: dict[str, Any]) -> tuple[dict[str, Any] | list[Any], dict[str, Any]]:
    url = f"{base_url}?{urlencode(parameters)}"
    request = Request(url, headers={"User-Agent": "nyc-taxi-weather-feasibility/1.0"})
    with urlopen(request, timeout=120) as response:
        body = response.read()
        status = response.status
        content_type = response.headers.get("Content-Type")
    if status != 200:
        raise RuntimeError(f"Unexpected HTTP status {status} for {url}")
    return json.loads(body), {
        "url": url,
        "http_status": status,
        "response_bytes": len(body),
        "content_type": content_type,
    }


def summarize_open_meteo(payload: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    hourly = payload["hourly"]
    variables = [name for name in hourly if name != "time"]
    return {
        **request,
        "json_top_level_keys": sorted(payload),
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
        "elevation": payload.get("elevation"),
        "timezone": payload.get("timezone"),
        "utc_offset_seconds": payload.get("utc_offset_seconds"),
        "hourly_units": payload.get("hourly_units"),
        "hourly_variables": variables,
        "hour_count": len(hourly["time"]),
        "first_time": hourly["time"][0],
        "last_time": hourly["time"][-1],
        "missing_counts": {
            name: sum(value is None for value in hourly[name]) for name in variables
        },
    }


def summarize_nasa_power(payload: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    parameters = payload["properties"]["parameter"]
    first_parameter = next(iter(parameters.values()))
    times = sorted(first_parameter)
    return {
        **request,
        "json_top_level_keys": sorted(payload),
        "geometry": payload.get("geometry"),
        "header": payload.get("header"),
        "hourly_variables": sorted(parameters),
        "hour_count": len(times),
        "first_time": times[0],
        "last_time": times[-1],
        "missing_counts": {
            name: sum(value is None or value == -999 for value in values.values())
            for name, values in parameters.items()
        },
    }


def percentile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(probability * len(ordered)) - 1)
    return ordered[index]


def summarize_spatial_variation(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    if len(payloads) != len(NYC_POINTS):
        raise ValueError("Open-Meteo multi-location response did not return every requested point")
    times = payloads[0]["hourly"]["time"]
    if any(item["hourly"]["time"] != times for item in payloads[1:]):
        raise ValueError("Open-Meteo multi-location timestamps are not aligned")

    temperature_ranges: list[float] = []
    wind_ranges: list[float] = []
    precipitation_ranges: list[float] = []
    precipitation_disagreement_hours = 0
    for index in range(len(times)):
        temperatures = [item["hourly"]["temperature_2m"][index] for item in payloads]
        winds = [item["hourly"]["wind_speed_10m"][index] for item in payloads]
        precipitation = [item["hourly"]["precipitation"][index] for item in payloads]
        temperature_ranges.append(max(temperatures) - min(temperatures))
        wind_ranges.append(max(winds) - min(winds))
        precipitation_ranges.append(max(precipitation) - min(precipitation))
        wet = [value > 0 for value in precipitation]
        if any(wet) and not all(wet):
            precipitation_disagreement_hours += 1

    return {
        "points": [
            {
                "name": name,
                "requested_latitude": coordinates[0],
                "requested_longitude": coordinates[1],
                "returned_latitude": payload["latitude"],
                "returned_longitude": payload["longitude"],
            }
            for (name, coordinates), payload in zip(NYC_POINTS.items(), payloads, strict=True)
        ],
        "hour_count": len(times),
        "temperature_range_c": {
            "mean": round(mean(temperature_ranges), 4),
            "p95": round(percentile(temperature_ranges, 0.95), 4),
            "maximum": round(max(temperature_ranges), 4),
            "hours_at_least_2c": sum(value >= 2 for value in temperature_ranges),
        },
        "wind_speed_range_kmh": {
            "mean": round(mean(wind_ranges), 4),
            "p95": round(percentile(wind_ranges, 0.95), 4),
            "maximum": round(max(wind_ranges), 4),
        },
        "precipitation_range_mm": {
            "mean": round(mean(precipitation_ranges), 4),
            "p95": round(percentile(precipitation_ranges, 0.95), 4),
            "maximum": round(max(precipitation_ranges), 4),
            "wet_dry_disagreement_hours": precipitation_disagreement_hours,
        },
    }


def run_checks() -> dict[str, Any]:
    central = {"latitude": 40.7128, "longitude": -74.0060}
    open_meteo_base = {
        **central,
        "start_date": "2025-01-01",
        "end_date": "2025-01-02",
        "hourly": ",".join(OPEN_METEO_VARIABLES),
        "timezone": "UTC",
    }

    observed, observed_request = fetch_json(
        "https://archive-api.open-meteo.com/v1/archive", open_meteo_base
    )
    historical_forecast, historical_request = fetch_json(
        "https://historical-forecast-api.open-meteo.com/v1/forecast", open_meteo_base
    )
    single_run, single_run_request = fetch_json(
        "https://single-runs-api.open-meteo.com/v1/forecast",
        {
            **central,
            "hourly": ",".join(OPEN_METEO_VARIABLES),
            "models": "ecmwf_ifs",
            "run": "2025-01-01T00:00",
            "forecast_days": 1,
            "timezone": "UTC",
        },
    )
    nasa, nasa_request = fetch_json(
        "https://power.larc.nasa.gov/api/temporal/hourly/point",
        {
            "parameters": "T2M,RH2M,PRECTOTCORR,WS10M,WD10M",
            "community": "RE",
            "longitude": central["longitude"],
            "latitude": central["latitude"],
            "start": "20250101",
            "end": "20250102",
            "format": "JSON",
            "time-standard": "UTC",
        },
    )

    latitudes = ",".join(str(point[0]) for point in NYC_POINTS.values())
    longitudes = ",".join(str(point[1]) for point in NYC_POINTS.values())
    spatial, spatial_request = fetch_json(
        "https://archive-api.open-meteo.com/v1/archive",
        {
            "latitude": latitudes,
            "longitude": longitudes,
            "start_date": "2025-01-01",
            "end_date": "2025-01-31",
            "hourly": "temperature_2m,precipitation,wind_speed_10m",
            "timezone": "UTC",
        },
    )
    if not isinstance(spatial, list):
        raise TypeError("Expected a list response for the multi-location request")

    return {
        "open_meteo_historical_weather": summarize_open_meteo(observed, observed_request),
        "open_meteo_historical_forecast": summarize_open_meteo(
            historical_forecast, historical_request
        ),
        "open_meteo_single_run": summarize_open_meteo(single_run, single_run_request),
        "nasa_power_hourly": summarize_nasa_power(nasa, nasa_request),
        "open_meteo_nyc_spatial_assessment": {
            **spatial_request,
            **summarize_spatial_variation(spatial),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--quiet", action="store_true", help="Do not print JSON to stdout")
    args = parser.parse_args()
    results = run_checks()
    rendered = json.dumps(results, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    if not args.quiet:
        print(rendered)


if __name__ == "__main__":
    main()
