"""Prove a Spark standalone cluster can round-trip Parquet through MinIO S3A."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType


SMOKE_ROOT = "s3a://bigdata/_infrastructure_smoke"
OUTPUT_PATH = f"{SMOKE_ROOT}/spark_minio_round_trip"

EXPECTED_SCHEMA = StructType(
    [
        StructField("record_id", IntegerType(), nullable=True),
        StructField("label", StringType(), nullable=True),
        StructField("is_valid", BooleanType(), nullable=True),
    ]
)

EXPECTED_VALUES = [
    (1, "alpha", True),
    (2, "beta", False),
    (3, "gamma", True),
]


def main() -> None:
    spark = SparkSession.builder.appName("spark-minio-infrastructure-smoke").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    smoke_root = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(SMOKE_ROOT)
    filesystem = smoke_root.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())

    try:
        source = spark.createDataFrame(EXPECTED_VALUES, schema=EXPECTED_SCHEMA)
        source.write.mode("overwrite").parquet(OUTPUT_PATH)

        restored = spark.read.parquet(OUTPUT_PATH)
        actual_values = [
            (row.record_id, row.label, row.is_valid)
            for row in restored.orderBy("record_id").collect()
        ]

        assert restored.schema == EXPECTED_SCHEMA, (
            f"schema mismatch: expected={EXPECTED_SCHEMA.simpleString()} "
            f"actual={restored.schema.simpleString()}"
        )
        assert restored.count() == len(EXPECTED_VALUES), "row count mismatch"
        assert actual_values == EXPECTED_VALUES, (
            f"value mismatch: expected={EXPECTED_VALUES!r} actual={actual_values!r}"
        )

        print(
            "SMOKE_TEST_OK "
            f"path={OUTPUT_PATH} rows={len(actual_values)} "
            f"schema={restored.schema.simpleString()} values={actual_values!r}"
        )
    finally:
        filesystem.delete(smoke_root, True)
        spark.stop()


if __name__ == "__main__":
    main()
