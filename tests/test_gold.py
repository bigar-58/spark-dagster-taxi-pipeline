from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.io.readers import read_taxi_zone_csv, read_yellow_taxi_csv
from taxi_pipeline.paths import TAXI_ZONE_SAMPLE_FILE, YELLOW_TAXI_SAMPLE_FILE
from taxi_pipeline.quality.checks import assert_unique_non_null_key
from taxi_pipeline.transforms.bronze import build_bronze_taxi_trips
from taxi_pipeline.transforms.gold import build_taxi_zone_dim, enrich_trips_with_pickup_zone
from taxi_pipeline.transforms.silver import build_silver_taxi_trips, split_valid_and_invalid_trips


@pytest.fixture
def enriched_valid_trips(spark: SparkSession) -> DataFrame:
    raw_trips_df = read_yellow_taxi_csv(spark=spark, input_path=YELLOW_TAXI_SAMPLE_FILE)

    bronze_df = build_bronze_taxi_trips(
        raw_trips_df,
        batch_id="gold-test-batch",
        ingested_at=datetime(2024, 1, 3, 12, 0, tzinfo=UTC),
    )

    silver_df = build_silver_taxi_trips(bronze_df)
    valid_df, _ = split_valid_and_invalid_trips(silver_df)  # No need for invalid

    raw_zones_df = read_taxi_zone_csv(spark=spark, input_path=TAXI_ZONE_SAMPLE_FILE)
    zones_df = build_taxi_zone_dim(raw_zones_df)

    return enrich_trips_with_pickup_zone(valid_df, zones_df)


def test_zone_enrichment_preserves_valid_trip_count(enriched_valid_trips: DataFrame) -> None:
    """Gold layer should not filter out any existing data from the silver layer"""
    assert enriched_valid_trips.count() == 3

    row = (
        enriched_valid_trips.filter(F.col("pickup_location_id") == 236)
        .select("pickup_borough", "pickup_zone", "_zone_lookup_matched")
        .first()
    )

    assert row is not None
    assert row["pickup_borough"] == "Manhattan"
    assert row["pickup_zone"] == "Upper East Side North"
    assert row["_zone_lookup_matched"] is True


def test_unknown_zone_does_not_drop_trip(enriched_valid_trips: DataFrame) -> None:
    """Assert that trips with unmatched pickup_loaction_id in zones returns Unknown"""
    unknown_trip = (
        enriched_valid_trips.limit(1)
        .drop("pickup_borough", "pickup_zone", "pickup_service_zone", "_zone_lookup_matched")
        .withColumn("pickup_location_id", F.lit(999))
    )

    zones = enriched_valid_trips.sparkSession.createDataFrame(
        [(236, "Manhattan", "Example", "Yellow Zone")],
        ["location_id", "borough", "zone", "service_zone"],
    )

    result = enrich_trips_with_pickup_zone(unknown_trip, zones).first()

    assert result is not None
    assert result["pickup_zone"] == "Unknown"
    assert result["_zone_lookup_matched"] is False


def test_zone_key_check_rejects_duplicates(spark: SparkSession) -> None:
    duplicate_zones = spark.createDataFrame(
        [
            (236, "Zone A"),
            (236, "Zone B"),
        ],
        ["location_id", "zone"],
    )

    with pytest.raises(ValueError, match="duplicate"):
        assert_unique_non_null_key(duplicate_zones, key_column="location_id", dataset_name="test zones")
