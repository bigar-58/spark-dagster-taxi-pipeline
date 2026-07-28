from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.schemas import TAXI_ZONE_RAW_SCHEMA, YELLOW_TAXI_RAW_SCHEMA


def read_yellow_taxi_csv(spark: SparkSession, input_path: Path) -> DataFrame:
    """
    General utility function to ingest yellow taxi CSV data (ingestion to bronze)
    """
    if not input_path.is_file():
        raise FileNotFoundError(f"Taxi input path not found: {input_path}")

    return (
        spark.read.schema(YELLOW_TAXI_RAW_SCHEMA)
        .option("header", True)
        .option("mode", "FAILFAST")
        .csv(str(input_path))
        .withColumn("_source_file_path", F.input_file_name())
    )


def read_parquet_dataset(spark: SparkSession, input_path: Path) -> DataFrame:
    """Reads an existing parquet dataset at input_path"""

    if not input_path.exists():
        raise FileNotFoundError("Parquet file not found")

    return spark.read.parquet(str(input_path))


def read_taxi_zone_csv(spark: SparkSession, input_path: Path) -> DataFrame:
    """
    Utility function to read taxi-zone look up based on expected schema
    """

    if not input_path.is_file():
        raise FileNotFoundError(f"Taxi zone input path not found: {input_path}")

    return (
        spark.read.schema(TAXI_ZONE_RAW_SCHEMA)
        .option("header", True)
        .option("mode", "FAILFAST")
        .csv(str(input_path))
        .withColumn("_source_file_path", F.input_file_name())
    )
