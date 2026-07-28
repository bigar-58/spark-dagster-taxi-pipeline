from __future__ import annotations

from taxi_pipeline.io.readers import read_parquet_dataset
from taxi_pipeline.io.writers import (
    overwrite_parquet,
    overwrite_partitioned_parquet,
)
from taxi_pipeline.paths import (
    YELLOW_TAXI_BRONZE_DIR,
    YELLOW_TAXI_SILVER_INVALID_DIR,
    YELLOW_TAXI_SILVER_VALID_DIR,
    ensure_data_directories,
)
from taxi_pipeline.spark import create_spark_session
from taxi_pipeline.transforms.silver import (
    build_silver_taxi_trips,
    split_valid_and_invalid_trips,
)


def main():
    """With existing bronze dataset create valid/invalid Silver layer"""
    ensure_data_directories()
    spark = create_spark_session("silver-yellow-taxi")

    try:
        bronze_df = read_parquet_dataset(spark=spark, input_path=YELLOW_TAXI_BRONZE_DIR)

        silver_df = build_silver_taxi_trips(bronze_df=bronze_df)
        valid_df, invalid_df = split_valid_and_invalid_trips(silver_df=silver_df)

        # Partition by year, month as this is the most deterministic set of keys.
        overwrite_partitioned_parquet(
            valid_df,
            YELLOW_TAXI_SILVER_VALID_DIR,
            partition_columns=["pickup_year", "pickup_month"],
        )

        # For invalid data we can simply do a full overwrite
        overwrite_parquet(invalid_df, YELLOW_TAXI_SILVER_INVALID_DIR)

        print(f"Valid silver layer data written to: {YELLOW_TAXI_SILVER_VALID_DIR}")
        print(f"Invalid silver layer data written to: {YELLOW_TAXI_SILVER_INVALID_DIR}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
