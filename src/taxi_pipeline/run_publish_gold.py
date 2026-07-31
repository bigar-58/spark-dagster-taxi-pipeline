from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession

from taxi_pipeline.config import PostgresSettings
from taxi_pipeline.io.readers import read_parquet_dataset
from taxi_pipeline.paths import (
    DAILY_ZONE_METRICS_GOLD_DIR,
    HOURLY_DEMAND_METRICS_GOLD_DIR
)
from taxi_pipeline.publishers.postgres import publish_gold_marts
from taxi_pipeline.spark import create_spark_session

@dataclass(frozen=True)
class GoldPublishRunResult:
    """Summary of a completed gold -> postgres publication"""
    run_id: str
    daily_zone_input_path: Path
    hourly_demand_input_path: Path
    date_from: date
    date_to: date
    daily_zone_row_count: int
    hourly_demand_row_count: int


def run_publish_gold_stage(
    spark: SparkSession,
    *,
    settings: PostgresSettings,
    run_id: str,
    daily_zone_input_path: Path = DAILY_ZONE_METRICS_GOLD_DIR,
    hourly_demand_input_path: Path = HOURLY_DEMAND_METRICS_GOLD_DIR
) -> GoldPublishRunResult:
    
    if not run_id.strip():
        raise ValueError("run_id cannot be empty")

    daily_zone_df = read_parquet_dataset(spark=spark, input_path=daily_zone_input_path)
    hourly_demand_df = read_parquet_dataset(spark=spark, input_path=hourly_demand_input_path)
    
    publish_result = publish_gold_marts(daily_zone_df=daily_zone_df, hourly_demand_df=hourly_demand_df, settings=settings, run_id=run_id)
    
    return GoldPublishRunResult(
        run_id=publish_result.run_id,
        daily_zone_input_path=daily_zone_input_path,
        hourly_demand_input_path=hourly_demand_input_path,
        date_from=publish_result.date_from,
        date_to=publish_result.date_to,
        daily_zone_row_count=publish_result.daily_zone_row_count,
        hourly_demand_row_count=publish_result.hourly_demand_row_count
    )

def main():
    """Handles the publishing of Gold layer parquet files into Postgres"""
    settings = PostgresSettings.from_env()
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"gold-{timestamp}-{uuid4().hex[:8]}"

    spark = create_spark_session("publish-gold-to-postgres")

    try:
        
        result = run_publish_gold_stage(spark=spark, settings=settings, run_id=run_id)

        print(
            f"Published gold marts for {result.date_from} through {result.date_to}. Daily Zone Rows: {result.daily_zone_row_count}, Hourly rows: {result.hourly_demand_row_count}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
