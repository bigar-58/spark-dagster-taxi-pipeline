from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.schemas import YELLOW_TAXI_RAW_SCHEMA


def read_yellow_taxi_csv(spark: SparkSession, input_path: Path) -> DataFrame:
    """
    General utility function to ingest yellow taxi CSV data
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
