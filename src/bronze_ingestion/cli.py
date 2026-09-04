"""Command-line entry point for Bronze ingestion and validation."""

from __future__ import annotations

import argparse
from pathlib import Path

from .core import (
    INVENTORY_KEY,
    MANIFEST_KEY,
    MISSING_COVERAGE_KEY,
    SUMMARY_KEY,
    VALIDATION_KEY,
    HttpClient,
    ObjectStore,
    build_summary,
    checkpoint,
    entry_index,
    ingest_references,
    ingest_taxi,
    ingest_weather,
    new_manifest,
    required_weather_plan,
    summarize_weather_payload,
    unavailable_weather_coverage,
    validate_manifest_objects,
    weather_object_key,
)


def write_local(output_dir: Path, name: str, content: bytes) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / name).write_bytes(content)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("/output"))
    parser.add_argument(
        "--request-delay-seconds",
        type=float,
        default=3.8,
        help="Delay between five-coordinate weather requests (keeps weighted usage below 5,000/hour)",
    )
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--deep-verify", action="store_true")
    parser.add_argument("--finalize-missing-coverage", action="store_true")
    args = parser.parse_args()

    store = ObjectStore()
    previous_manifest = store.get_json(MANIFEST_KEY)

    if args.finalize_missing_coverage:
        if previous_manifest is None:
            raise RuntimeError(f"No existing manifest at {MANIFEST_KEY}")
        previous = entry_index(previous_manifest)
        weather = []
        weather_failures = []
        for run, required_targets in required_weather_plan().items():
            response_key = weather_object_key(run)
            unavailable_key = response_key.replace("response.json", "unavailable.json")
            if response_key in previous:
                entry = dict(previous[response_key])
                entry.update(
                    summarize_weather_payload(store.get_bytes(response_key), run, required_targets)
                )
                weather.append(entry)
            elif unavailable_key in previous:
                entry = dict(previous[unavailable_key])
                entry.update(unavailable_weather_coverage(run, required_targets))
                weather_failures.append(entry)
            else:
                raise RuntimeError(f"Weather plan artifact is missing: {run.isoformat()}")
        previous_manifest["weather"] = weather
        previous_manifest["weather_failures"] = weather_failures
        checkpoint(store, previous_manifest, args.output_dir)
        http_stats = previous_manifest.get("source_http_this_execution", {})
        summary, _ = build_summary(previous_manifest, http_stats)
        summary["bronze_objects_after_ingestion"] = store.list("bronze/")
        summary_content = store.put_json(SUMMARY_KEY, summary)
        coverage = {
            "generated_at": summary["generated_at"],
            "expected_run_count": summary["weather"]["expected_run_count"],
            "present_run_count": summary["weather"]["present_run_count"],
            "unavailable_run_count": summary["weather"]["unavailable_run_count"],
            "unavailable_runs": summary["weather"]["unavailable_runs"],
            "missing_predictor_slot_count": summary["weather"]["missing_predictor_slot_count"],
            "missing_required_target_point_horizon_count": summary["weather"]["missing_required_target_point_horizon_count"],
            "missing_target_hour_count": summary["weather"]["missing_target_hour_count"],
            "missing_target_hours": summary["weather"]["missing_target_hours"],
            "by_run": summary["weather"]["missing_coverage_by_run"],
            "by_target_hour": summary["weather"]["missing_coverage_by_target_hour"],
            "by_horizon_hours": summary["weather"]["missing_coverage_by_horizon_hours"],
            "by_point": summary["weather"]["missing_coverage_by_point"],
            "by_variable": summary["weather"]["missing_coverage_by_variable"],
            "full_slot_detail_manifest_key": MANIFEST_KEY,
            "imputation_performed": False,
            "observed_or_reanalysis_substitution": False,
        }
        coverage_content = store.put_json(MISSING_COVERAGE_KEY, coverage)
        write_local(args.output_dir, "bronze_ingestion_summary.json", summary_content)
        write_local(args.output_dir, "weather_missing_coverage.json", coverage_content)
        print(
            f"MISSING_COVERAGE_OK runs={len(weather) + len(weather_failures)} "
            f"unavailable={len(weather_failures)} "
            f"missing_slots={coverage['missing_predictor_slot_count']}",
            flush=True,
        )
        return

    if args.validate_only:
        if previous_manifest is None:
            raise RuntimeError(f"No existing manifest at {MANIFEST_KEY}")
        report = validate_manifest_objects(store, previous_manifest, deep_verify=args.deep_verify)
        content = store.put_json(VALIDATION_KEY, report)
        write_local(args.output_dir, "bronze_validation_report.json", content)
        print(
            f"BRONZE_VALIDATION_OK objects={report['checked_object_count']} "
            f"bytes={report['checked_bytes']} deep={report['deep_checksum_verification']}",
            flush=True,
        )
        return

    previous = entry_index(previous_manifest or {})
    manifest = new_manifest()
    if previous_manifest is not None:
        manifest["execution_history"] = list(previous_manifest.get("execution_history", []))
        if previous_manifest.get("source_http_this_execution"):
            manifest["execution_history"].append(previous_manifest["source_http_this_execution"])
    http = HttpClient()

    try:
        manifest["taxi"] = ingest_taxi(store, http, previous)
        checkpoint(store, manifest, args.output_dir)

        manifest["reference"] = ingest_references(store, http, previous)
        checkpoint(store, manifest, args.output_dir)

        def weather_checkpoint(results, failures):
            manifest["weather"] = results
            manifest["weather_failures"] = failures
            checkpoint(store, manifest, args.output_dir)

        manifest["weather"], manifest["weather_failures"] = ingest_weather(
            store,
            http,
            previous,
            request_delay_seconds=args.request_delay_seconds,
            checkpoint_callback=weather_checkpoint,
        )
        manifest["source_http_this_execution"] = http.stats.as_dict()
        checkpoint(store, manifest, args.output_dir)
    except Exception:
        manifest["source_http_this_execution"] = http.stats.as_dict()
        checkpoint(store, manifest, args.output_dir)
        raise

    summary, raw_inventory = build_summary(manifest, http.stats.as_dict())
    inventory_content = store.put_json(INVENTORY_KEY, raw_inventory)
    summary["bronze_objects_after_ingestion"] = store.list("bronze/")
    summary_content = store.put_json(SUMMARY_KEY, summary)
    write_local(args.output_dir, "raw_object_inventory.json", inventory_content)
    write_local(args.output_dir, "bronze_ingestion_summary.json", summary_content)
    print(
        f"BRONZE_INGESTION_OK taxi_months={summary['taxi']['present_month_count']} "
        f"taxi_bytes={summary['taxi']['total_bytes']} "
        f"weather_runs={summary['weather']['present_run_count']} "
        f"weather_bytes={summary['weather']['total_bytes']} "
        f"references={summary['reference']['present_object_count']}",
        flush=True,
    )


if __name__ == "__main__":
    main()
