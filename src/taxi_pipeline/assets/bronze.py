from datetime import UTC, datetime
from typing import Literal

import dagster as dg
from pyspark.sql import SparkSession

from taxi_pipeline.run_bronze import run_bronze_stage
from taxi_pipeline.sources.yellow_taxi import resolve_yellow_taxi_input

class BronzeTaxiConfig(dg.Config):
    """Select the source ingested by the bronze asset"""
    source: Literal["sample", "tlc"] = "sample"
    year: int = 2024
    month: int = 1


@dg.asset(group_name="bronze", required_resource_keys={"spark"}, description="Raw taxi data with basic metadata")
def bronze_taxi_trips(context: dg.AssetExecutionContext, config: BronzeTaxiConfig) -> dg.MaterializeResult:
    """Materialize the bronze layer for yellow taxi data"""
    spark: SparkSession = context.resources.spark
    ingested_at = datetime.now(UTC)
    batch_id = context.run_id
    input_spec = resolve_yellow_taxi_input(source=config.source, year=config.year, month=config.month)
    
    context.log.info("Starting bronze ingestion for batch '%s'", batch_id)

    result = run_bronze_stage(spark, batch_id=batch_id, ingested_at=ingested_at, input_spec=input_spec)
    context.log.info("Wrote '%s' bronze rows to '%s'", result.row_count, result.output_path)

    metadata = {
        "row_count": result.row_count,
        "batch_id": result.batch_id,
        "ingested_at": result.ingested_at.isoformat(),
        "source_mode": result.source_mode,
        "input_format": result.input_format,
        "source_file_size_bytes": result.source_file_size_bytes,
        "input_file": dg.MetadataValue.path(str(result.input_path)),
        "output_dataset": dg.MetadataValue.path(str(result.output_path)),
    }

    if result.source_year:
        metadata["source_year"] = result.source_year

    if result.source_month:
        metadata["source_month"] = result.source_month

    if result.source_url:
        metadata["source_url"] = dg.MetadataValue.url(
            result.source_url
        )

    return dg.MaterializeResult(metadata=metadata)