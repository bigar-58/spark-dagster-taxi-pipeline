from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_BRONZE_COLUMNS: frozenset[str] = frozenset(
    {
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "_source_file_path",
        "_batch_id",
        "_ingested_at",
    }
)


def build_silver_taxi_trips(bronze_df: DataFrame) -> DataFrame:
    """Take existing/input bronze data and map to valid types"""
    missing_cols = sorted(REQUIRED_BRONZE_COLUMNS - set(bronze_df.columns))

    if missing_cols:
        raise ValueError(f"Bronze DataFram has missing column(s): {missing_cols}")

    # explicitly alias to ensure that output has correct field names
    casted_df = bronze_df.select(
        F.col("VendorID").cast("integer").alias("vendor_id"),
        F.to_timestamp(
            "tpep_pickup_datetime",
            "yyyy-MM-dd HH:mm:ss",
        ).alias("pickup_timestamp"),
        F.to_timestamp(
            "tpep_dropoff_datetime",
            "yyyy-MM-dd HH:mm:ss",
        ).alias("dropoff_timestamp"),
        F.col("passenger_count").cast("integer").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("integer").alias("rate_code_id"),
        F.col("store_and_fwd_flag").alias("store_and_fwd_flag"),
        F.col("PULocationID").cast("integer").alias("pickup_location_id"),
        F.col("DOLocationID").cast("integer").alias("dropoff_location_id"),
        F.col("payment_type").cast("integer").alias("payment_type"),
        F.col("fare_amount").cast("decimal(12,2)").alias("fare_amount"),
        F.col("extra").cast("decimal(12,2)").alias("extra"),
        F.col("mta_tax").cast("decimal(12,2)").alias("mta_tax"),
        F.col("tip_amount").cast("decimal(12,2)").alias("tip_amount"),
        F.col("tolls_amount").cast("decimal(12,2)").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("decimal(12,2)").alias("improvement_surcharge"),
        F.col("total_amount").cast("decimal(12,2)").alias("total_amount"),
        F.col("congestion_surcharge").cast("decimal(12,2)").alias("congestion_surcharge"),
        F.col("airport_fee").cast("decimal(12,2)").alias("airport_fee"),
        F.col("_source_file_path"),
        F.col("_batch_id"),
        F.col("_ingested_at"),
    )

    # Enrich the existing data set with some basic extracts/aggregations for easy access
    casted_df = (
        casted_df.withColumn("pickup_date", F.to_date("pickup_timestamp"))
        .withColumn("pickup_hour", F.hour("pickup_timestamp"))
        .withColumn("pickup_year", F.year("pickup_timestamp"))
        .withColumn("pickup_month", F.month("pickup_timestamp"))
        .withColumn(
            "trip_duration_minutes",
            (F.col("dropoff_timestamp").cast("long") - F.col("pickup_timestamp").cast("long")) / F.lit(60.0),
        )
        .withColumn(
            "fare_per_mile",
            F.when(
                F.col("trip_distance") > 0,
                F.col("fare_amount").cast("double") / F.col("trip_distance"),
            ),
        )
    )

    # Set of all conditions that may invalidate inputted bronze data after transformation
    invalid_reasons = F.array(
        F.when(F.col("pickup_timestamp").isNull(), F.lit("invalid_pickup_timestamp")),
        F.when(F.col("dropoff_timestamp").isNull(), F.lit("invalid_dropoff_timestamp")),
        F.when(
            F.col("pickup_timestamp").isNotNull()
            & F.col("dropoff_timestamp").isNotNull()
            & (F.col("dropoff_timestamp") <= F.col("pickup_timestamp")),
            F.lit("dropoff_not_after_pickup"),
        ),
        F.when(
            F.col("trip_distance").isNull() | (F.col("trip_distance") <= 0),
            F.lit("non_positive_trip_distance"),
        ),
        F.when(
            F.col("fare_amount").isNull() | (F.col("fare_amount") < 0),
            F.lit("invalid_fare_amount"),
        ),
        F.when(
            F.col("total_amount").isNull() | (F.col("total_amount") < 0),
            F.lit("invalid_total_amount"),
        ),
        F.when(
            F.col("pickup_location_id").isNull(),
            F.lit("missing_pickup_location"),
        ),
        F.when(
            F.col("dropoff_location_id").isNull(),
            F.lit("missing_dropoff_location"),
        ),
    )

    return casted_df.withColumn("_invalid_reasons", F.filter(invalid_reasons, lambda reason: reason.isNotNull())).withColumn(
        "is_valid", F.size("_invalid_reasons") == 0
    )


def split_valid_and_invalid_trips(silver_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split Silver records without losing rejected source rows."""

    valid_df = silver_df.filter(F.col("is_valid"))
    invalid_df = silver_df.filter(~F.col("is_valid"))

    return valid_df, invalid_df
