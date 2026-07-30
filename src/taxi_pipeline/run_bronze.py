from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from taxi_pipeline.io.readers import read_yellow_taxi_csv
from taxi_pipeline.io.writers import overwrite_parquet
from taxi_pipeline.paths import YELLOW_TAXI_BRONZE_DIR, YELLOW_TAXI_SAMPLE_FILE, ensure_data_directories
from taxi_pipeline.spark import create_spark_session
from taxi_pipeline.transforms.bronze import build_bronze_taxi_trips


@dataclass(frozen=True)
class BronzeRunResult:
    """Summary of a completed Bronze materialization."""

    batch_id: str
    ingested_at: datetime
    input_path: Path
    output_path: Path
    row_count: int


def run_bronze_stage(
    spark: SparkSession,
    *,
    batch_id: str,
    ingested_at: datetime,
    input_path: Path = YELLOW_TAXI_SAMPLE_FILE,
    output_path: Path = YELLOW_TAXI_BRONZE_DIR,
) -> BronzeRunResult:
    """Read, transform, and persist the raw taxi data into the bronze dataset"""
    ensure_data_directories()

    raw_df = read_yellow_taxi_csv(spark=spark, input_path=input_path)
    bronze_df = build_bronze_taxi_trips(raw_df, batch_id=batch_id, ingested_at=ingested_at).persist()

    try:
        row_count = bronze_df.count()
        overwrite_parquet(bronze_df, output_path)
    finally:
        bronze_df.unpersist()

    return BronzeRunResult(
        batch_id=batch_id, ingested_at=ingested_at, input_path=input_path, output_path=output_path, row_count=row_count
    )


def main():
    """Materialize the local sample set into Bronze taxi dataset"""
    ensure_data_directories()

    ingested_at = datetime.now(UTC)
    batch_id = ingested_at.strftime("%Y%m%dT%H%M%SZ")

    spark = create_spark_session("bronze-yellow-taxi")

    try:
        result = run_bronze_stage(spark, batch_id=batch_id, ingested_at=ingested_at)

        print(f"Bronze taxi data successfully written to {result.output_path}")
    finally:
        spark.stop()  # In the event of any erros end current Spark session


if __name__ == "__main__":
    main()
