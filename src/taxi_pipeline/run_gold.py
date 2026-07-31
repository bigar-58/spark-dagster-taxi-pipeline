from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.io.readers import read_parquet_dataset
from taxi_pipeline.io.writers import overwrite_partitioned_parquet
from taxi_pipeline.paths import (
    DAILY_ZONE_METRICS_GOLD_DIR,
    HOURLY_DEMAND_METRICS_GOLD_DIR,
    TAXI_ZONE_REFERENCE_DIR,
    YELLOW_TAXI_SILVER_VALID_DIR,
    ensure_data_directories
)
from taxi_pipeline.quality.checks import assert_unique_non_null_key
from taxi_pipeline.spark import create_spark_session
from taxi_pipeline.transforms.gold import (
    build_daily_zone_metrics,
    build_hourly_demand_metrics,
    enrich_trips_with_pickup_zone
)

@dataclass(frozen=True)
class GoldRunResult:
    silver_input_path: Path
    zone_lookup_input_path: Path
    daily_zone_output_path: Path
    hourly_demand_output_path: Path

    valid_trip_count: int
    unmatched_zone_count: int
    daily_zone_row_count: int
    hourly_demand_row_count: int

    date_from: date | None
    date_to: date | None
    
    @property
    def zone_lookup_match_rate(self) -> float:
        """Return the fraction of trips matched to a taxi zone."""

        if self.valid_trip_count == 0:
            return 0.0
        
        matched_count = self.valid_trip_count - self.unmatched_zone_count
        
        return matched_count / self.valid_trip_count
    
    
def run_gold_stage(
    spark: SparkSession,
    *,
    silver_input_path: Path = YELLOW_TAXI_SILVER_VALID_DIR,
    zone_lookup_input_path: Path = TAXI_ZONE_REFERENCE_DIR,
    daily_zone_output_path: Path = DAILY_ZONE_METRICS_GOLD_DIR,
    hourly_demand_output_path: Path = HOURLY_DEMAND_METRICS_GOLD_DIR
) -> GoldRunResult:
    """
    Build and persist hourly and daily gold taxi datasets
    """
    
    valid_trips_df = read_parquet_dataset(spark=spark, input_path=silver_input_path)
    
    taxi_zones_df = read_parquet_dataset(spark=spark, input_path=zone_lookup_input_path)
    
    assert_unique_non_null_key(taxi_zones_df, key_column="location_id", dataset_name="taxi-zone dimension")
    
    enriched_trips_df = enrich_trips_with_pickup_zone(valid_trips_df=valid_trips_df, taxi_zones_df=taxi_zones_df)
    
    daily_zone_metrics, hourly_demand_metrics = None, None
    
    try: 
        input_summary = enriched_trips_df.agg(
            F.count("*").alias("valid_trip_count"),
            F.sum(
                F.when(
                    F.col("_zone_lookup_matched"),
                    F.lit(0),
                ).otherwise(F.lit(1))
            ).alias("unmatched_zone_count"),
            F.min("pickup_date").alias("date_from"),
            F.max("pickup_date").alias("date_to")
        ).first()
        
        if not input_summary:
            raise RuntimeError("Unable to fetch valid enriched gold input")

        
        valid_trip_count = int(input_summary["valid_trip_count"])
        unmatched_zone_count = int(input_summary["unmatched_zone_count"] or 0)
        
        daily_zone_metrics = build_daily_zone_metrics(enriched_trips_df=enriched_trips_df).persist(StorageLevel.MEMORY_AND_DISK)
        hourly_demand_metrics = build_hourly_demand_metrics(enriched_trips_df=enriched_trips_df).persist(StorageLevel.MEMORY_AND_DISK)
        
        daily_zone_row_count = daily_zone_metrics.count()
        hourly_demand_row_count = hourly_demand_metrics.count()

        
        overwrite_partitioned_parquet(daily_zone_metrics, daily_zone_output_path, partition_columns=["pickup_year", "pickup_month"])
        overwrite_partitioned_parquet(hourly_demand_metrics, hourly_demand_output_path, partition_columns=["pickup_year", "pickup_month"])        
        
        return GoldRunResult(
            silver_input_path=silver_input_path,
            zone_lookup_input_path=zone_lookup_input_path,
            daily_zone_output_path=daily_zone_output_path,
            hourly_demand_output_path=hourly_demand_output_path,
            valid_trip_count=valid_trip_count,
            unmatched_zone_count=unmatched_zone_count,
            daily_zone_row_count=daily_zone_row_count,
            hourly_demand_row_count=hourly_demand_row_count,
            date_from=input_summary["date_from"],
            date_to=input_summary["date_to"]
        )
    finally: 
        if daily_zone_metrics is not None:
            daily_zone_metrics.unpersist()

        if hourly_demand_metrics is not None:
            hourly_demand_metrics.unpersist()

        enriched_trips_df.unpersist()
        
        
        
        
def main():
    """Materialize Gold layer for taxi analytics datasets"""

    ensure_data_directories()
    spark = create_spark_session("gold-yellow-taxi")


    try:
        result = run_gold_stage(spark=spark)

        print(f"Daily zone metrics written to: {result.daily_zone_output_path}")

        print(f"Hourly demand metrics written to: {result.hourly_demand_output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
