from __future__ import annotations

from pyspark import StorageLevel

from taxi_pipeline.io.readers import (
    read_parquet_dataset,
    read_taxi_zone_csv,
)
from taxi_pipeline.io.writers import (
    overwrite_partitioned_parquet,
)
from taxi_pipeline.paths import (
    DAILY_ZONE_METRICS_GOLD_DIR,
    HOURLY_DEMAND_METRICS_GOLD_DIR,
    TAXI_ZONE_SAMPLE_FILE,
    YELLOW_TAXI_SILVER_VALID_DIR,
    ensure_data_directories,
)
from taxi_pipeline.quality.checks import (
    assert_unique_non_null_key,
)
from taxi_pipeline.spark import create_spark_session
from taxi_pipeline.transforms.gold import (
    build_daily_zone_metrics,
    build_hourly_demand_metrics,
    build_taxi_zone_dim,
    enrich_trips_with_pickup_zone,
)


def main():
    """Materialize Gold layer for taxi analytics datasets"""

    ensure_data_directories()
    spark = create_spark_session("gold-yellow-taxi")

    enriched_trips_df = None

    try:
        # Read current silver layer data set
        valid_trips_df = read_parquet_dataset(spark=spark, input_path=YELLOW_TAXI_SILVER_VALID_DIR)

        raw_zones_df = read_taxi_zone_csv(spark=spark, input_path=TAXI_ZONE_SAMPLE_FILE)

        taxi_zones_df = build_taxi_zone_dim(raw_zones_df)
        assert_unique_non_null_key(
            taxi_zones_df, key_column="location_id", dataset_name="taxi-zone dimension"
        )

        enriched_trips_df = enrich_trips_with_pickup_zone(
            valid_trips_df=valid_trips_df, taxi_zones_df=taxi_zones_df
        ).persist(StorageLevel.MEMORY_AND_DISK)

        daily_zone_metrics_df = build_daily_zone_metrics(enriched_trips_df)
        hourly_demand_metrics_df = build_hourly_demand_metrics(enriched_trips_df)

        overwrite_partitioned_parquet(
            daily_zone_metrics_df,
            DAILY_ZONE_METRICS_GOLD_DIR,
            partition_columns=["pickup_year", "pickup_month"],
        )

        print(f"Daily zone metrics written to: {DAILY_ZONE_METRICS_GOLD_DIR}")

        overwrite_partitioned_parquet(
            hourly_demand_metrics_df,
            HOURLY_DEMAND_METRICS_GOLD_DIR,
            partition_columns=["pickup_year", "pickup_month"],
        )

        print(f"Hourly demand metrics written to: {HOURLY_DEMAND_METRICS_GOLD_DIR}")
    finally:
        if enriched_trips_df is not None:
            enriched_trips_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
