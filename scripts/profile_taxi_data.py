"""Profile an NYC TLC Yellow Taxi Parquet file without modifying its records."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


IMPORTANT_FIELDS = (
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "total_amount",
)


def count_true(mask: pa.Array) -> int:
    """Count true values while treating null predicates as false."""
    result = pc.sum(pc.cast(pc.fill_null(mask, False), pa.int64())).as_py()
    return int(result or 0)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_zone_lookup(path: Path) -> tuple[dict[int, dict[str, str]], dict[str, int]]:
    zones: dict[int, dict[str, str]] = {}
    boroughs: Counter[str] = Counter()
    with path.open(encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source):
            location_id = int(row["LocationID"])
            zones[location_id] = row
            boroughs[row["Borough"]] += 1
    if not zones:
        raise ValueError("Taxi Zone lookup contains no LocationIDs")
    return zones, dict(sorted(boroughs.items()))


def update_value_counts(target: Counter[int], values: pa.Array) -> None:
    for item in pc.value_counts(values).to_pylist():
        value = item["values"]
        if value is not None:
            target[int(value)] += int(item["counts"])


def profile_file(
    parquet_path: Path,
    zone_lookup_path: Path,
    expected_month: str,
    source_url: str,
    zone_lookup_url: str,
) -> dict[str, Any]:
    month_start = datetime.strptime(expected_month, "%Y-%m")
    if month_start.month == 12:
        month_end = datetime(month_start.year + 1, 1, 1)
    else:
        month_end = datetime(month_start.year, month_start.month + 1, 1)

    zones, borough_counts = load_zone_lookup(zone_lookup_path)
    zone_ids = set(zones)
    min_zone_id, max_zone_id = min(zone_ids), max(zone_ids)
    parquet = pq.ParquetFile(parquet_path)
    schema = parquet.schema_arrow
    missing = sorted(set(IMPORTANT_FIELDS) - set(schema.names))
    if missing:
        raise ValueError(f"Required profile fields missing: {', '.join(missing)}")

    null_counts = {name: 0 for name in schema.names}
    location_counts = {"PULocationID": Counter(), "DOLocationID": Counter()}
    pickup_hours_by_zone: dict[int, set[datetime]] = defaultdict(set)
    timestamp_stats: dict[str, dict[str, Any]] = {
        "tpep_pickup_datetime": {"min": None, "max": None, "outside_expected_month": 0},
        "tpep_dropoff_datetime": {"min": None, "max": None, "outside_expected_month": 0},
    }
    quality = Counter()
    duration_min_seconds: float | None = None
    duration_max_seconds: float | None = None
    processed_rows = 0

    for batch in parquet.iter_batches(batch_size=100_000):
        processed_rows += batch.num_rows
        for index, name in enumerate(schema.names):
            null_counts[name] += batch.column(index).null_count

        for name in timestamp_stats:
            values = batch[name]
            batch_min = pc.min(values).as_py()
            batch_max = pc.max(values).as_py()
            if batch_min is not None:
                current = timestamp_stats[name]["min"]
                timestamp_stats[name]["min"] = batch_min if current is None else min(current, batch_min)
            if batch_max is not None:
                current = timestamp_stats[name]["max"]
                timestamp_stats[name]["max"] = batch_max if current is None else max(current, batch_max)
            before = pc.less(values, pa.scalar(month_start, type=values.type))
            after = pc.greater_equal(values, pa.scalar(month_end, type=values.type))
            timestamp_stats[name]["outside_expected_month"] += count_true(pc.or_(before, after))

        for name in location_counts:
            update_value_counts(location_counts[name], batch[name])

        pickup_hours = pc.floor_temporal(batch["tpep_pickup_datetime"], unit="hour").to_pylist()
        pickup_zones = batch["PULocationID"].to_pylist()
        for zone, pickup_hour in zip(pickup_zones, pickup_hours, strict=True):
            if (
                zone is not None
                and pickup_hour is not None
                and month_start <= pickup_hour < month_end
            ):
                pickup_hours_by_zone[int(zone)].add(pickup_hour)

        distance = batch["trip_distance"]
        quality["trip_distance_zero"] += count_true(pc.equal(distance, 0))
        quality["trip_distance_negative"] += count_true(pc.less(distance, 0))
        quality["trip_distance_over_100_miles"] += count_true(pc.greater(distance, 100))

        fare = batch["fare_amount"]
        quality["fare_amount_zero"] += count_true(pc.equal(fare, 0))
        quality["fare_amount_negative"] += count_true(pc.less(fare, 0))
        quality["fare_amount_over_500"] += count_true(pc.greater(fare, 500))

        total = batch["total_amount"]
        quality["total_amount_zero"] += count_true(pc.equal(total, 0))
        quality["total_amount_negative"] += count_true(pc.less(total, 0))
        quality["total_amount_over_1000"] += count_true(pc.greater(total, 1000))

        pickup = batch["tpep_pickup_datetime"]
        dropoff = batch["tpep_dropoff_datetime"]
        valid_pair = pc.and_(pc.is_valid(pickup), pc.is_valid(dropoff))
        quality["duration_zero"] += count_true(pc.and_(valid_pair, pc.equal(dropoff, pickup)))
        quality["duration_negative"] += count_true(pc.and_(valid_pair, pc.less(dropoff, pickup)))
        durations = pc.subtract(dropoff, pickup)
        over_24h = pc.greater(durations, pa.scalar(timedelta(hours=24), type=durations.type))
        quality["duration_over_24_hours"] += count_true(pc.and_(valid_pair, over_24h))
        duration_ticks = pc.cast(durations, pa.int64())
        unit_scale = {"s": 1.0, "ms": 1e-3, "us": 1e-6, "ns": 1e-9}[durations.type.unit]
        batch_duration_min = pc.min(duration_ticks).as_py()
        batch_duration_max = pc.max(duration_ticks).as_py()
        if batch_duration_min is not None:
            value = batch_duration_min * unit_scale
            duration_min_seconds = (
                value if duration_min_seconds is None else min(duration_min_seconds, value)
            )
        if batch_duration_max is not None:
            value = batch_duration_max * unit_scale
            duration_max_seconds = (
                value if duration_max_seconds is None else max(duration_max_seconds, value)
            )

    if processed_rows != parquet.metadata.num_rows:
        raise RuntimeError(
            f"Processed {processed_rows} rows but Parquet metadata reports {parquet.metadata.num_rows}"
        )

    location_summary: dict[str, Any] = {}
    for name, counts in location_counts.items():
        null_count = null_counts[name]
        not_in_lookup = sum(count for zone, count in counts.items() if zone not in zone_ids)
        out_of_range = sum(
            count for zone, count in counts.items() if zone < min_zone_id or zone > max_zone_id
        )
        distribution = [
            {
                "location_id": zone,
                "count": count,
                "percent_of_rows": round(count * 100 / processed_rows, 6),
            }
            for zone, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        ]
        location_summary[name] = {
            "distinct_non_null": len(counts),
            "null": null_count,
            "not_in_official_lookup": not_in_lookup,
            "outside_lookup_id_range": out_of_range,
            "distribution": distribution,
        }

    for stats in timestamp_stats.values():
        stats["min"] = stats["min"].isoformat(sep=" ") if stats["min"] else None
        stats["max"] = stats["max"].isoformat(sep=" ") if stats["max"] else None

    expected_hours = int((month_end - month_start).total_seconds() // 3600)
    nyc_boroughs = {"Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"}
    eligible_zone_ids = {zone for zone, row in zones.items() if row["Borough"] in nyc_boroughs}
    eligible_pickups = sum(location_counts["PULocationID"][zone] for zone in eligible_zone_ids)
    if eligible_pickups == 0:
        parquet.close()
        raise ValueError("No pickups were found in the five NYC boroughs")
    ranked_eligible_zones = sorted(
        eligible_zone_ids,
        key=lambda zone: (-location_counts["PULocationID"][zone], zone),
    )
    coverage_sets: dict[str, Any] = {}
    for threshold in (0.90, 0.95, 0.99):
        selected: list[int] = []
        cumulative = 0
        for zone in ranked_eligible_zones:
            selected.append(zone)
            cumulative += location_counts["PULocationID"][zone]
            if eligible_pickups and cumulative / eligible_pickups >= threshold:
                break
        coverage_sets[f"{int(threshold * 100)}_percent"] = {
            "minimum_zone_count": len(selected),
            "pickup_count": cumulative,
            "pickup_coverage_percent": round(cumulative * 100 / eligible_pickups, 6),
            "minimum_selected_active_hour_percent": round(
                min(len(pickup_hours_by_zone[zone]) for zone in selected) * 100 / expected_hours,
                6,
            ),
            "zone_ids": selected,
        }

    zone_hour_sparsity = [
        {
            "location_id": zone,
            "borough": zones[zone]["Borough"],
            "zone": zones[zone]["Zone"],
            "pickup_count": location_counts["PULocationID"][zone],
            "active_hours": len(pickup_hours_by_zone[zone]),
            "zero_demand_hours": expected_hours - len(pickup_hours_by_zone[zone]),
            "active_hour_percent": round(
                len(pickup_hours_by_zone[zone]) * 100 / expected_hours,
                6,
            ),
        }
        for zone in ranked_eligible_zones
    ]

    metadata_row_groups = parquet.metadata.num_row_groups
    metadata_created_by = parquet.metadata.created_by
    parquet.close()
    size_bytes = parquet_path.stat().st_size
    return {
        "source": {
            "url": source_url,
            "file_name": parquet_path.name,
            "format": "Apache Parquet",
            "size_bytes": size_bytes,
            "size_mib": round(size_bytes / 1024 / 1024, 4),
            "sha256": sha256_file(parquet_path),
            "expected_month": expected_month,
        },
        "parquet": {
            "rows": processed_rows,
            "row_groups": metadata_row_groups,
            "created_by": metadata_created_by,
            "schema": [
                {"name": field.name, "type": str(field.type), "nullable": field.nullable}
                for field in schema
            ],
        },
        "timestamps": timestamp_stats,
        "null_counts": null_counts,
        "location_ids": location_summary,
        "quality_flags": {
            **dict(sorted(quality.items())),
            "duration_min_seconds": duration_min_seconds,
            "duration_max_seconds": duration_max_seconds,
        },
        "pickup_zone_hour_sparsity": {
            "expected_hours": expected_hours,
            "eligible_boroughs": sorted(nyc_boroughs),
            "eligible_zone_count": len(eligible_zone_ids),
            "eligible_pickup_count": eligible_pickups,
            "coverage_sets": coverage_sets,
            "zones": zone_hour_sparsity,
        },
        "zone_lookup": {
            "url": zone_lookup_url,
            "file_name": zone_lookup_path.name,
            "row_count": len(zone_ids),
            "minimum_location_id": min_zone_id,
            "maximum_location_id": max_zone_id,
            "borough_counts": borough_counts,
        },
        "threshold_notes": {
            "trip_distance_over_100_miles": "Screening flag only; not an automatic rejection rule.",
            "fare_amount_over_500": "Screening flag only; not an automatic rejection rule.",
            "total_amount_over_1000": "Screening flag only; not an automatic rejection rule.",
            "duration_over_24_hours": "Screening flag only; not an automatic rejection rule.",
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("parquet", type=Path)
    parser.add_argument("zone_lookup", type=Path)
    parser.add_argument("--expected-month", required=True, help="Expected pickup month as YYYY-MM")
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--zone-lookup-url", required=True)
    parser.add_argument("--output", type=Path, help="Optional JSON output path")
    parser.add_argument("--quiet", action="store_true", help="Do not print JSON to stdout")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    profile = profile_file(
        args.parquet,
        args.zone_lookup,
        args.expected_month,
        args.source_url,
        args.zone_lookup_url,
    )
    rendered = json.dumps(profile, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    if not args.quiet:
        print(rendered)


if __name__ == "__main__":
    main()
