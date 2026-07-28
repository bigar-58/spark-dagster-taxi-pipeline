from __future__ import annotations

from datetime import UTC, datetime

from taxi_pipeline.io.readers import read_yellow_taxi_csv
from taxi_pipeline.io.writers import overwrite_parquet
from taxi_pipeline.paths import (
    YELLOW_TAXI_BRONZE_DIR,
    YELLOW_TAXI_SAMPLE_FILE,
    ensure_data_directories,
)
from taxi_pipeline.spark import create_spark_session
from taxi_pipeline.transforms.bronze import build_bronze_taxi_trips


def main():
    """Materialize the local sample set into Bronze taxi dataset"""
    ensure_data_directories()

    ingested_at = datetime.now(UTC)
    batch_id = ingested_at.strftime("%Y%m%dT%H%M%SZ")

    spark = create_spark_session("bronze-yellow-taxi")

    try:
        raw_df = read_yellow_taxi_csv(spark=spark, input_path=YELLOW_TAXI_SAMPLE_FILE)

        bronze_df = build_bronze_taxi_trips(raw_df, batch_id=batch_id, ingested_at=ingested_at)

        overwrite_parquet(bronze_df, YELLOW_TAXI_BRONZE_DIR)

        print(f"Bronze taxi data successfully written to {YELLOW_TAXI_BRONZE_DIR}")
    finally:
        spark.stop()  # In the event of any erros end current Spark session


if __name__ == "__main__":
    main()
