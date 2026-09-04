"""Build the validated Bronze-to-Silver Spark transformation layer."""

from __future__ import annotations

import io
import json
import math
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import shapefile
from pyproj import CRS, Transformer
from shapely.geometry import shape
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

from silver_etl.transforms import (
    FIVE_BOROUGHS,
    FORECAST_HORIZONS_HOURS,
    NYC_POINTS,
    PUBLICATION_LAG_HOURS,
    TARGET_END_EXCLUSIVE_UTC,
    TARGET_START_UTC,
    WEATHER_VARIABLES,
    canonical_hour_axis,
    classify_taxi,
    complete_demand_grid,
    complete_weather_records,
    expected_weather_grid,
    nearest_weather_point,
)


BUCKET = "bigdata"
TAXI_INPUT = f"s3a://{BUCKET}/bronze/taxi/yellow_tripdata_2025-*.parquet"
ZONE_LOOKUP = f"s3a://{BUCKET}/bronze/reference/taxi_zone_lookup.csv"
ZONE_GEOGRAPHY = f"s3a://{BUCKET}/bronze/reference/taxi_zones.zip"
WEATHER_INPUT = f"s3a://{BUCKET}/bronze/weather/ecmwf_ifs/run=*/response.json"
TAXI_RECORDS_OUTPUT = f"s3a://{BUCKET}/silver/taxi_clean/records"
DEMAND_OUTPUT = f"s3a://{BUCKET}/silver/taxi_clean/hourly_demand"
ZONE_MAP_OUTPUT = f"s3a://{BUCKET}/silver/taxi_clean/zone_weather_map"
WEATHER_OUTPUT = f"s3a://{BUCKET}/silver/weather_clean/records"
JOIN_OUTPUT = f"s3a://{BUCKET}/silver/demand_weather/records"
REPORT_OUTPUT = f"s3a://{BUCKET}/silver/manifests/silver_etl_report.json"


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("nyc-taxi-weather-silver-etl")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def read_zone_lookup(spark: SparkSession):
    schema = T.StructType(
        [
            T.StructField("LocationID", T.IntegerType(), False),
            T.StructField("Borough", T.StringType(), True),
            T.StructField("Zone", T.StringType(), True),
            T.StructField("service_zone", T.StringType(), True),
        ]
    )
    return spark.read.option("header", True).schema(schema).csv(ZONE_LOOKUP)


def derive_zone_centroids(spark: SparkSession):
    archive = spark.read.format("binaryFile").load(ZONE_GEOGRAPHY).select("content").first()
    if archive is None:
        raise RuntimeError("Taxi Zone geographic archive is missing")
    with tempfile.TemporaryDirectory(prefix="taxi-zones-") as directory:
        with zipfile.ZipFile(io.BytesIO(bytes(archive.content))) as source:
            source.extractall(directory)
        root = Path(directory)
        shp_path = next(root.rglob("*.shp"))
        prj_path = next(root.rglob("*.prj"))
        source_crs = CRS.from_wkt(prj_path.read_text(encoding="utf-8"))
        transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
        reader = shapefile.Reader(str(shp_path))
        rows = []
        for item in reader.iterShapeRecords():
            attributes = item.record.as_dict()
            location_id = int(attributes.get("LocationID", attributes.get("location_i")))
            geometry = shape(item.shape.__geo_interface__)
            centroid = geometry.centroid
            longitude, latitude = transformer.transform(centroid.x, centroid.y)
            weather_point, distance_km = nearest_weather_point(latitude, longitude)
            rows.append((location_id, latitude, longitude, weather_point, distance_km))
    return spark.createDataFrame(
        rows,
        "location_id int, centroid_latitude double, centroid_longitude double, "
        "weather_point string, weather_point_distance_km double",
    )


def parse_weather(spark: SparkSession):
    raw = (
        spark.read.option("multiLine", True)
        .json(WEATHER_INPUT)
        .withColumn("source_file", F.input_file_name())
    )
    run_token = F.regexp_extract("source_file", r"run=([^/]+)", 1)
    run_text = F.regexp_replace(run_token, r"T(\d{2})-(\d{2})$", "T$1:$2")
    point_udf = F.udf(lambda lat, lon: nearest_weather_point(float(lat), float(lon))[0], T.StringType())
    zipped = F.arrays_zip(
        F.col("hourly.time"),
        *[F.col(f"hourly.{variable}") for variable in WEATHER_VARIABLES],
    )
    exploded = raw.withColumn("weather_point", point_udf("latitude", "longitude")).withColumn(
        "hour", F.explode(zipped)
    )
    return exploded.select(
        F.to_timestamp(run_text, "yyyy-MM-dd'T'HH:mm").alias("run_initialization_utc"),
        F.to_timestamp(F.col("hour.time"), "yyyy-MM-dd'T'HH:mm").alias("target_time_utc"),
        "weather_point",
        F.col("latitude").cast("double").alias("provider_latitude"),
        F.col("longitude").cast("double").alias("provider_longitude"),
        F.col("timezone").alias("provider_timezone"),
        F.col("utc_offset_seconds").cast("int").alias("provider_utc_offset_seconds"),
        *[F.col(f"hour.{variable}").alias(variable) for variable in WEATHER_VARIABLES],
    )


def write_json_object(spark: SparkSession, uri: str, value: dict) -> None:
    content = json.dumps(value, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = filesystem.create(path, True)
    try:
        stream.write(bytearray(content))
    finally:
        stream.close()


def silver_inventory(spark: SparkSession) -> dict:
    path = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{BUCKET}/silver/")
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    iterator = filesystem.listFiles(path, True)
    objects = []
    while iterator.hasNext():
        status = iterator.next()
        key = status.getPath().toString().split(f"/{BUCKET}/", 1)[-1]
        if key.endswith("/.keep") or "/_temporary/" in key:
            continue
        objects.append({"object_key": key, "bytes": int(status.getLen())})
    return {
        "object_count_before_report": len(objects),
        "total_bytes_before_report": sum(item["bytes"] for item in objects),
        "objects": sorted(objects, key=lambda item: item["object_key"]),
    }


def duplicate_count(frame, keys: list[str]) -> int:
    return frame.groupBy(*keys).count().filter(F.col("count") > 1).count()


def main() -> None:
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    lookup = read_zone_lookup(spark)
    five_borough_zones = lookup.filter(F.col("Borough").isin(*FIVE_BOROUGHS))
    official_zone_count = five_borough_zones.select("LocationID").distinct().count()

    centroids = derive_zone_centroids(spark).join(
        five_borough_zones.select(F.col("LocationID").alias("location_id"), "Borough", "Zone"),
        "location_id",
        "inner",
    )
    centroids.orderBy("location_id").write.mode("overwrite").parquet(ZONE_MAP_OUTPUT)

    taxi = (
        spark.read.parquet(TAXI_INPUT)
        .withColumn("source_file", F.input_file_name())
        .withColumn("source_month", F.regexp_extract("source_file", r"yellow_tripdata_(2025-\d{2})", 1))
    )
    classified = classify_taxi(taxi, lookup)
    (
        classified.repartition(48, "record_status", "source_month")
        .write.mode("overwrite")
        .partitionBy("record_status", "source_month")
        .parquet(TAXI_RECORDS_OUTPUT)
    )
    classified_disk = spark.read.parquet(TAXI_RECORDS_OUTPUT)
    reason_rows = classified_disk.groupBy("record_status", "record_reason").count().collect()
    reason_counts = {
        (row.record_status, row.record_reason or "accepted"): int(row["count"])
        for row in reason_rows
    }
    taxi_input_rows = sum(reason_counts.values())
    accepted_rows = sum(count for (status, _), count in reason_counts.items() if status == "accepted")
    rejected_rows = sum(count for (status, _), count in reason_counts.items() if status == "rejected")
    quarantined_rows = sum(count for (status, _), count in reason_counts.items() if status == "quarantined")
    accepted = classified_disk.filter(F.col("record_status") == "accepted")

    zone_totals = [
        (int(row.location_id), int(row.pickups))
        for row in accepted.groupBy(F.col("PULocationID").cast("int").alias("location_id"))
        .agg(F.count(F.lit(1)).alias("pickups"))
        .collect()
    ]
    zone_totals.sort(key=lambda item: (-item[1], item[0]))
    cumulative = 0
    selected_zone_ids = []
    for location_id, pickups in zone_totals:
        cumulative += pickups
        selected_zone_ids.append(location_id)
        if cumulative / accepted_rows >= 0.95:
            break
    selected_coverage = cumulative / accepted_rows

    hours = canonical_hour_axis(spark)
    demand = complete_demand_grid(accepted, five_borough_zones, hours)
    demand.repartition(48, "location_id").write.mode("overwrite").parquet(DEMAND_OUTPUT)
    demand_disk = spark.read.parquet(DEMAND_OUTPUT)
    demand_metrics = demand_disk.agg(
        F.count(F.lit(1)).alias("grid_rows"),
        F.sum(F.when(F.col("demand_available") & (F.col("pickup_count") == 0), 1).otherwise(0)).alias(
            "zero_demand_rows"
        ),
        F.sum(F.when(~F.col("demand_available"), 1).otherwise(0)).alias("unavailable_rows"),
    ).first()
    active_stats = [
        {
            "location_id": int(row.location_id),
            "available_hours": int(row.available_hours),
            "active_hours": int(row.active_hours),
            "active_hour_rate": float(row.active_hours / row.available_hours),
        }
        for row in demand_disk.filter(F.col("location_id").isin(*selected_zone_ids))
        .groupBy("location_id")
        .agg(
            F.sum(F.col("demand_available").cast("int")).alias("available_hours"),
            F.sum(
                F.when(F.col("demand_available") & (F.col("pickup_count") > 0), 1).otherwise(0)
            ).alias("active_hours"),
        )
        .orderBy("location_id")
        .collect()
    ]

    weather_points = spark.createDataFrame(
        [(slug,) for slug, _, _ in NYC_POINTS], "weather_point string"
    )
    parsed_weather = parse_weather(spark)
    parsed_duplicate_count = duplicate_count(
        parsed_weather, ["run_initialization_utc", "target_time_utc", "weather_point"]
    )
    expected_weather = expected_weather_grid(spark, weather_points)
    weather = (
        complete_weather_records(expected_weather, parsed_weather)
        .withColumn("provider", F.lit("Open-Meteo"))
        .withColumn("model", F.lit("ecmwf_ifs"))
    )
    leakage_rows = weather.filter(
        F.col("run_initialization_utc").cast("long") + PUBLICATION_LAG_HOURS * 3600
        > F.col("target_time_utc").cast("long") - F.col("horizon_hours") * 3600
    ).count()
    weather.repartition(24, "horizon_hours").write.mode("overwrite").partitionBy(
        "horizon_hours"
    ).parquet(WEATHER_OUTPUT)
    weather_disk = spark.read.parquet(WEATHER_OUTPUT)
    weather_metrics = weather_disk.agg(
        F.count(F.lit(1)).alias("expected_records"),
        F.sum(F.col("source_response_available").cast("int")).alias("source_present_records"),
        F.sum((~F.col("source_response_available")).cast("int")).alias("source_unavailable_records"),
        F.sum(F.col("any_weather_missing").cast("int")).alias("records_with_missing_weather"),
    ).first()
    weather_duplicate_count = duplicate_count(
        weather_disk, ["target_time_utc", "horizon_hours", "weather_point"]
    )

    zone_map = spark.read.parquet(ZONE_MAP_OUTPUT).select(
        "location_id",
        "centroid_latitude",
        "centroid_longitude",
        "weather_point",
        "weather_point_distance_km",
    )
    joined = demand_disk.join(zone_map, "location_id", "inner").join(
        weather_disk, ["target_time_utc", "weather_point"], "left"
    )
    join_count = joined.count()
    expected_join_count = int(demand_metrics.grid_rows) * len(FORECAST_HORIZONS_HOURS)
    unmatched_join_rows = joined.filter(F.col("horizon_hours").isNull()).count()
    join_duplicate_count = duplicate_count(
        joined, ["location_id", "target_time_utc", "horizon_hours"]
    )
    joined.repartition(48, "horizon_hours").write.mode("overwrite").partitionBy(
        "horizon_hours"
    ).parquet(JOIN_OUTPUT)

    if parsed_duplicate_count or weather_duplicate_count or join_duplicate_count:
        raise RuntimeError("Duplicate primary keys detected in Silver outputs")
    if leakage_rows:
        raise RuntimeError("Weather leakage rule violation detected")
    if join_count != expected_join_count or unmatched_join_rows:
        raise RuntimeError("Demand-weather join cardinality validation failed")

    inventory = silver_inventory(spark)
    report = {
        "report_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "time_axis": {
            "taxi_wall_timezone_convention": "America/New_York",
            "target_start_utc_inclusive": TARGET_START_UTC.isoformat(),
            "target_end_utc_exclusive": TARGET_END_EXCLUSIVE_UTC.isoformat(),
            "hour_count": 8760,
            "fall_back_target_hours_unavailable": [
                "2025-11-02T05:00:00Z",
                "2025-11-02T06:00:00Z",
            ],
        },
        "taxi": {
            "bronze_input_rows": taxi_input_rows,
            "accepted_rows": accepted_rows,
            "rejected_rows": rejected_rows,
            "quarantined_rows": quarantined_rows,
            "reason_counts": {
                f"{status}:{reason}": count
                for (status, reason), count in sorted(reason_counts.items())
            },
            "official_five_borough_zone_count": official_zone_count,
            "five_borough_pickup_count": accepted_rows,
        },
        "demand": {
            "grid_rows": int(demand_metrics.grid_rows),
            "zero_demand_rows": int(demand_metrics.zero_demand_rows),
            "dst_unavailable_rows": int(demand_metrics.unavailable_rows),
            "primary_key_duplicate_count": duplicate_count(
                demand_disk, ["location_id", "target_time_utc"]
            ),
            "coverage_rule": "smallest demand-ranked five-borough zone set reaching at least 95%",
            "selected_zone_count": len(selected_zone_ids),
            "selected_zone_ids": selected_zone_ids,
            "selected_pickups": cumulative,
            "selected_coverage": selected_coverage,
            "selected_zone_active_hour_statistics": active_stats,
        },
        "weather": {
            "expected_records": int(weather_metrics.expected_records),
            "source_present_records": int(weather_metrics.source_present_records),
            "source_unavailable_records": int(weather_metrics.source_unavailable_records),
            "records_with_missing_weather": int(weather_metrics.records_with_missing_weather),
            "primary_key_duplicate_count": weather_duplicate_count,
            "parsed_primary_key_duplicate_count": parsed_duplicate_count,
            "leakage_violation_count": leakage_rows,
            "publication_lag_hours": PUBLICATION_LAG_HOURS,
            "variables": list(WEATHER_VARIABLES),
            "imputation_performed": False,
            "observed_weather_substitution": False,
        },
        "join": {
            "expected_rows": expected_join_count,
            "actual_rows": join_count,
            "unmatched_rows": unmatched_join_rows,
            "primary_key_duplicate_count": join_duplicate_count,
            "many_to_many_expansion": join_count != expected_join_count,
        },
        "schemas": {
            "taxi_records": classified_disk.schema.jsonValue(),
            "hourly_demand": demand_disk.schema.jsonValue(),
            "zone_weather_map": zone_map.schema.jsonValue(),
            "weather_clean": weather_disk.schema.jsonValue(),
            "demand_weather": joined.schema.jsonValue(),
        },
        "silver_object_inventory": inventory,
    }
    write_json_object(spark, REPORT_OUTPUT, report)
    local_output = Path("/opt/project/output")
    local_output.mkdir(parents=True, exist_ok=True)
    (local_output / "silver_etl_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        "SILVER_ETL_OK "
        f"taxi_input={taxi_input_rows} accepted={accepted_rows} rejected={rejected_rows} "
        f"quarantined={quarantined_rows} demand_grid={int(demand_metrics.grid_rows)} "
        f"weather={int(weather_metrics.expected_records)} joined={join_count}",
        flush=True,
    )
    spark.stop()


if __name__ == "__main__":
    main()
