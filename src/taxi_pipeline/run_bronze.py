from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from taxi_pipeline.io.readers import read_yellow_taxi_source
from taxi_pipeline.io.writers import overwrite_parquet
from taxi_pipeline.paths import YELLOW_TAXI_BRONZE_DIR, ensure_data_directories
from taxi_pipeline.sources.yellow_taxi import (
    YellowTaxiInput,
    ensure_yellow_taxi_input_available,
    resolve_yellow_taxi_input
)
from taxi_pipeline.spark import create_spark_session
from taxi_pipeline.transforms.bronze import build_bronze_taxi_trips

@dataclass(frozen=True)
class BronzeRunResult:
    """Summary of a completed Bronze materialization."""

    batch_id: str
    ingested_at: datetime

    source_mode: str
    input_format: str
    source_year: int | None
    source_month: int | None
    source_url: str | None

    input_path: Path
    source_file_size_bytes: int
    output_path: Path
    row_count: int


def run_bronze_stage(
    spark: SparkSession,
    *,
    batch_id: str,
    ingested_at: datetime,
    input_spec: YellowTaxiInput | None = None,
    output_path: Path = YELLOW_TAXI_BRONZE_DIR
) -> BronzeRunResult:
    """Read, transform, and persist the raw taxi data into the bronze dataset"""
    ensure_data_directories()

    resolved_input = input_spec if input_spec else resolve_yellow_taxi_input()
    available_input = ensure_yellow_taxi_input_available(resolved_input)
    
    raw_df = read_yellow_taxi_source(spark=spark, input_spec=available_input)
    bronze_df = build_bronze_taxi_trips(raw_df, batch_id=batch_id, ingested_at=ingested_at).persist()

    try:
        row_count = bronze_df.count()
        overwrite_parquet(bronze_df, output_path)
    finally:
        bronze_df.unpersist()

    return BronzeRunResult(
        batch_id=batch_id,
        ingested_at=ingested_at,
        source_mode=available_input.source,
        input_format=available_input.input_format,
        source_year=available_input.year,
        source_month=available_input.month,
        source_url=available_input.source_url,
        input_path=available_input.path,
        source_file_size_bytes=(
            available_input.path.stat().st_size
        ),
        output_path=output_path,
        row_count=row_count
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
