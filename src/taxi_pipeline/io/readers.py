from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.schemas import (
    TAXI_ZONE_RAW_SCHEMA,
    YELLOW_TAXI_RAW_COLUMNS,
    YELLOW_TAXI_RAW_SCHEMA
)
from taxi_pipeline.sources.yellow_taxi import YellowTaxiInput


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


def read_yellow_taxi_parquet(spark: SparkSession, input_path: Path) -> DataFrame:
    """Read TLC parquet while preserving the Bronze string contract"""
    
    if not input_path.exists():
        raise FileNotFoundError(f"Taxi parquet input not found: {input_path}")
    
    raw_df = spark.read.parquet(str(input_path))
    
    if "Airport_fee" in raw_df.columns and "airport_fee" not in raw_df.columns: 
        raw_df = raw_df.withColumnRenamed("Airport_fee", "airport_fee")
        
    missing_columns = sorted(set(YELLOW_TAXI_RAW_COLUMNS) - set(raw_df.columns))
    
    if missing_columns:
        raise ValueError(f"Yellow taxi Parquet is missing required columns: {missing_columns}")
    
    source_columns = [column for column in raw_df.columns if column != "_source_file_path"]
    
    return raw_df.select(
        *[F.col(column).cast("string").alias(column) for column in source_columns],
        F.input_file_name().alias("_source_file_path")
    )
    

def read_yellow_taxi_source(spark: SparkSession, input_spec: YellowTaxiInput) -> DataFrame:
    """"Read yellow taxi input using declared format"""
    if input_spec.input_format == "csv":
        return read_yellow_taxi_csv(spark=spark, input_path=input_spec.path)
    
    if input_spec.input_format == "parquet":
        return read_yellow_taxi_parquet(spark=spark, input_path=input_spec.path)
    
    raise ValueError(f"Unsupported yellow taxi input format: {input_spec.input_format}")
    