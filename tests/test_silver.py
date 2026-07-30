from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.io.readers import read_yellow_taxi_csv
from taxi_pipeline.paths import YELLOW_TAXI_SAMPLE_FILE
from taxi_pipeline.transforms.bronze import build_bronze_taxi_trips
from taxi_pipeline.transforms.silver import build_silver_taxi_trips, split_valid_and_invalid_trips


@pytest.fixture
def silver_taxi_df(spark: SparkSession):
    """Reusable silver dataframe for remaining tests"""
    raw_df = read_yellow_taxi_csv(spark=spark, input_path=YELLOW_TAXI_SAMPLE_FILE)

    bronze_df = build_bronze_taxi_trips(
        raw_df,
        batch_id="test-batch",
        ingested_at=datetime(
            2024,
            1,
            3,
            12,
            0,
            tzinfo=UTC,
        ),
    )

    return build_silver_taxi_trips(bronze_df)


def test_silver_taxi_trips_casts_and_derives_fields(silver_taxi_df) -> None:
    """Ensure dataset enrichment + casting processed correctly"""
    row = (
        silver_taxi_df.filter(F.col("pickup_hour") == 8)
        .select("pickup_date", "pickup_hour", "trip_duration_minutes", "fare_per_mile")
        .first()
    )

    assert row is not None
    assert str(row["pickup_date"]) == "2024-01-01"
    assert row["pickup_hour"] == 8
    assert row["trip_duration_minutes"] == pytest.approx(20.0)
    assert row["fare_per_mile"] == pytest.approx(5.75)


def test_silver_taxi_trips_identifies_expected_invalid_rows(silver_taxi_df) -> None:
    """Ensure data split is correctly identifying validations"""
    valid_df, invalid_df = split_valid_and_invalid_trips(silver_taxi_df)

    assert valid_df.count() == 3
    assert invalid_df.count() == 2

    reasons = {reason for row in invalid_df.select("_invalid_reasons").collect() for reason in row["_invalid_reasons"]}

    assert "dropoff_not_after_pickup" in reasons
    assert "non_positive_trip_distance" in reasons
