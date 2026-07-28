from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_VALID_TRIP_COLUMNS: frozenset[str] = frozenset(
    {
        "pickup_date",
        "pickup_year",
        "pickup_month",
        "pickup_hour",
        "pickup_location_id",
        "trip_distance",
        "trip_duration_minutes",
        "fare_per_mile",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
        "_batch_id",
        "_ingested_at",
        "is_valid",
    }
)

REQUIRED_ZONE_COLUMNS: frozenset[str] = frozenset(
    {
        "LocationID",
        "Borough",
        "Zone",
        "service_zone",
    }
)


def build_taxi_zone_dim(raw_zone_df: DataFrame) -> DataFrame:
    """Standardize taxi zone data for downstream use"""

    missing_columns = sorted(REQUIRED_ZONE_COLUMNS - set(raw_zone_df.columns))

    if missing_columns:
        raise ValueError(f"Taxi-zone DataFram is missing columns: {missing_columns}")

    return raw_zone_df.select(
        F.col("LocationID").cast("integer").alias("location_id"),
        F.trim("Borough").alias("borough"),
        F.trim("Zone").alias("zone"),
        F.trim("service_zone").alias("service_zone"),
        F.col("_source_file_path"),
    )


def enrich_trips_with_pickup_zone(valid_trips_df: DataFrame, taxi_zones_df: DataFrame) -> DataFrame:
    """Add readable pickup-zone attributes without dropping unmatched trips."""
    missing_trip_columns = sorted(REQUIRED_VALID_TRIP_COLUMNS - set(valid_trips_df.columns))

    if missing_trip_columns:
        raise ValueError(f"Valid Silver DataFrame is missing columns: {missing_trip_columns}")

    trips = valid_trips_df.alias("trips")
    zones = taxi_zones_df.alias("zones")

    # Left-join due to also wanting trips that don't happend to have zone info matching in zone lookup.
    return trips.join(
        zones, F.col("trips.pickup_location_id") == F.col("zones.location_id"), how="left"
    ).select(
        "trips.*",
        F.coalesce(
            F.col("zones.borough"),
            F.lit("Unknown"),
        ).alias("pickup_borough"),
        F.coalesce(
            F.col("zones.zone"),
            F.lit("Unknown"),
        ).alias("pickup_zone"),
        F.coalesce(
            F.col("zones.service_zone"),
            F.lit("Unknown"),
        ).alias("pickup_service_zone"),
        F.col("zones.location_id").isNotNull().alias("_zone_lookup_matched"),
    )


def build_daily_zone_metrics(enriched_trips_df: DataFrame) -> DataFrame:
    """Aggregate trusted trips by pickup date and zone"""

    return (
        enriched_trips_df.groupBy(
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_location_id",
            "pickup_borough",
            "pickup_zone",
            "pickup_service_zone",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("gross_revenue_amount"),
            F.sum("fare_amount").alias("fare_revenue_amount"),
            F.sum("tip_amount").alias("total_tip_amount"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
            F.avg("fare_per_mile").alias("avg_fare_per_mile"),
            F.avg(
                F.when(
                    F.col("payment_type") == 1,
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
            ).alias("card_payment_share"),
            F.avg(
                F.when(
                    F.col("payment_type") == 2,
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
            ).alias("cash_payment_share"),
            F.countDistinct("_batch_id").alias("source_batch_count"),
            F.max("_ingested_at").alias("latest_source_ingested_at"),
        )
        .orderBy("pickup_date", F.desc("trip_count"), "pickup_location_id")
    )


def build_hourly_demand_metrics(enriched_trips_df: DataFrame) -> DataFrame:
    """Aggregate trusted trips by pickup date and hour"""

    return (
        enriched_trips_df.groupBy("pickup_date", "pickup_year", "pickup_month", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.countDistinct("pickup_location_id").alias("distinct_pickup_zones"),
            F.sum("total_amount").alias("gross_revenue_amount"),
            F.sum("fare_amount").alias("fare_revenue_amount"),
            F.sum("tip_amount").alias("total_tip_amount"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
            F.avg(
                F.when(
                    F.col("payment_type") == 1,
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
            ).alias("card_payment_share"),
            F.avg(
                F.when(
                    F.col("payment_type") == 2,
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
            ).alias("cash_payment_share"),
            F.countDistinct("_batch_id").alias("source_batch_count"),
            F.max("_ingested_at").alias("latest_source_ingested_at"),
        )
        .orderBy("pickup_date", "pickup_hour")
    )
