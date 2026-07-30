from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from taxi_pipeline.config import PostgresSettings
from taxi_pipeline.io.readers import read_parquet_dataset
from taxi_pipeline.paths import DAILY_ZONE_METRICS_GOLD_DIR, HOURLY_DEMAND_METRICS_GOLD_DIR
from taxi_pipeline.publishers.postgres import publish_gold_marts
from taxi_pipeline.spark import create_spark_session


def main():
    """Handles the publishing of Gold layer parquet files into Postgres"""
    settings = PostgresSettings.from_env()
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"gold-{timestamp}-{uuid4().hex[:8]}"

    spark = create_spark_session("publish-gold-to-postgres")

    try:
        daily_zone_df = read_parquet_dataset(spark=spark, input_path=DAILY_ZONE_METRICS_GOLD_DIR)
        hourly_demand_df = read_parquet_dataset(spark=spark, input_path=HOURLY_DEMAND_METRICS_GOLD_DIR)

        result = publish_gold_marts(daily_zone_df, hourly_demand_df, settings=settings, run_id=run_id)

        print(
            f"Published gold marts for {result.date_from} through {result.date_to}. Daily Zone Rows: {result.daily_zone_row_count}, Hourly rows: {result.hourly_demand_row_count}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
