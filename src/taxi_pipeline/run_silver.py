from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from taxi_pipeline.io.readers import read_parquet_dataset
from taxi_pipeline.io.writers import (
    overwrite_parquet,
    overwrite_partitioned_parquet
)
from taxi_pipeline.paths import (
    YELLOW_TAXI_BRONZE_DIR,
    YELLOW_TAXI_SILVER_INVALID_DIR,
    YELLOW_TAXI_SILVER_VALID_DIR,
    ensure_data_directories
)
from taxi_pipeline.spark import create_spark_session
from taxi_pipeline.transforms.silver import (
    build_silver_taxi_trips,
    split_valid_and_invalid_trips
)


@dataclass(frozen=True)
class SilverRunResult:
    bronze_input_path: Path
    valid_output_path: Path
    invalid_output_path: Path
    total_row_count: int
    valid_row_count: int
    invalid_row_count: int
    
    @property
    def invalid_row_rate(self) -> float:
        """
        Return proportion of silver rows that are invalid
        """
        if self.total_row_count == 0:
            return 0.0
        
        return self.invalid_row_count / self.total_row_count
        

def run_silver_stage(
    spark: SparkSession,
    *,
    bronze_input_path: Path = YELLOW_TAXI_BRONZE_DIR,
    valid_output_path: Path = YELLOW_TAXI_SILVER_VALID_DIR,
    invalid_output_path: Path = YELLOW_TAXI_SILVER_INVALID_DIR
) -> SilverRunResult:
    """Transform and persist valid and silver rows from bronze layer"""
    
    bronze_df = read_parquet_dataset(spark=spark, input_path=bronze_input_path)
    silver_df = build_silver_taxi_trips(bronze_df=bronze_df).persist()
    
    try:
        valid_df, invalid_df = split_valid_and_invalid_trips(silver_df=silver_df)
        
        valid_row_count = valid_df.count()
        invalid_row_count = invalid_df.count()
        total_row_count = valid_row_count + invalid_row_count
        
        overwrite_partitioned_parquet(valid_df, valid_output_path, partition_columns=["pickup_year", "pickup_month"])
        overwrite_parquet(invalid_df, invalid_output_path)
    finally: 
        silver_df.unpersist()
    
    return SilverRunResult(
        bronze_input_path=bronze_input_path,
        valid_output_path=valid_output_path,
        invalid_output_path=invalid_output_path,
        total_row_count=total_row_count,
        valid_row_count=valid_row_count,
        invalid_row_count=invalid_row_count
    )

def main():
    """With existing bronze dataset create valid/invalid Silver layer"""
    ensure_data_directories()
    spark = create_spark_session("silver-yellow-taxi")

    try:
        result = run_silver_stage(spark)

        print(f"Valid silver layer data written to: {result.valid_output_path}")
        print(f"Invalid silver layer data written to: {result.invalid_output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
