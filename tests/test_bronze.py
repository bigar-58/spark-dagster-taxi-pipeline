from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from taxi_pipeline.io.readers import read_yellow_taxi_csv
from taxi_pipeline.io.writers import overwrite_parquet
from taxi_pipeline.paths import YELLOW_TAXI_SAMPLE_FILE
from taxi_pipeline.schemas import YELLOW_TAXI_RAW_COLUMNS
from taxi_pipeline.transforms.bronze import build_bronze_taxi_trips
from taxi_pipeline.run_bronze import run_bronze_stage


def test_read_yellow_taxi_expected_schema(spark: SparkSession) -> None:
    """Ensure that we are actually ingesting the raw data according to expected schema"""
    result = read_yellow_taxi_csv(spark=spark, input_path=YELLOW_TAXI_SAMPLE_FILE)

    src_columns = tuple(column for column in result.columns if column != "_source_file_path")

    assert src_columns == YELLOW_TAXI_RAW_COLUMNS
    assert result.count() == 5

    for field in result.schema.fields:
        assert isinstance(field.dataType, StringType)  # All of the raw data should be ingested as a string


def test_read_yellow_taxi_src_file_path(spark: SparkSession) -> None:
    """Ensure source paths written to data frame on ingest"""
    result = read_yellow_taxi_csv(spark=spark, input_path=YELLOW_TAXI_SAMPLE_FILE)

    src_paths = [row["_source_file_path"] for row in result.select("_source_file_path").distinct().collect()]

    assert len(src_paths) == 1
    assert src_paths[0].endswith(YELLOW_TAXI_SAMPLE_FILE.name)  # File should point to input path


def test_read_yellow_taxi_missing_file_exception(spark: SparkSession, tmp_path: Path) -> None:
    """Ensure we fail gracefully when inputting non-existent file path"""
    missing_file = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError, match="not found"):
        read_yellow_taxi_csv(spark=spark, input_path=missing_file)


def test_build_bronze_taxi_trips_adds_batch_metadata(spark: SparkSession) -> None:
    """Ensure we can track down ingest time with metadata tag"""
    ingested_at = datetime(2024, 1, 15, 12, 30, tzinfo=UTC)

    raw_df = spark.createDataFrame([("file:///tmp/yellow_taxi.csv",)], ["_source_file_path"])

    result = build_bronze_taxi_trips(raw_df, batch_id="batch-20240115", ingested_at=ingested_at)

    row = result.select(
        "_source_file_path",
        "_batch_id",
        F.col("_ingested_at").cast("long").alias("ingested_at_epoch"),
    ).first()

    assert row is not None
    assert row["_source_file_path"] == "file:///tmp/yellow_taxi.csv"
    assert row["_batch_id"] == "batch-20240115"
    assert row["ingested_at_epoch"] == int(ingested_at.timestamp())


def test_overwrite_parquet_replaces_existing_data(spark: SparkSession, tmp_path: Path) -> None:
    output_path = tmp_path / "bronze-output"

    first_df = spark.createDataFrame(
        [(1, "first")],
        ["record_id", "value"],
    )
    second_df = spark.createDataFrame(
        [(2, "second")],
        ["record_id", "value"],
    )

    overwrite_parquet(first_df, output_path=output_path)
    overwrite_parquet(second_df, output_path=output_path)

    rows = spark.read.parquet(str(output_path)).collect()

    assert (len(rows)) == 1
    assert rows[0]["record_id"] == 2
    assert rows[0]["value"] == "second"


def test_run_bronze_stage_writes_dataset(spark: SparkSession, tmp_path: Path) -> None:
    ingested_at = datetime(2024, 1, 15, 12, 30, tzinfo=UTC)
    output_path = tmp_path / "bronze-taxi"

    result = run_bronze_stage(
        spark,
        batch_id="test-batch-20240115",
        ingested_at=ingested_at,
        input_path=YELLOW_TAXI_SAMPLE_FILE,
        output_path=output_path,
    )

    written_df = spark.read.parquet(str(output_path))

    assert result.row_count == 5
    assert result.output_path == output_path
    assert written_df.count() == 5

    batch_ids = [row["_batch_id"] for row in written_df.select("_batch_id").distinct().collect()]

    assert batch_ids == ["test-batch-20240115"]
