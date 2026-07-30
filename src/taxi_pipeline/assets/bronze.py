from datetime import UTC, datetime

import dagster as dg
from pyspark.sql import SparkSession

from taxi_pipeline.run_bronze import run_bronze_stage


@dg.asset(group_name="bronze", required_resource_keys={"spark"}, description="Raw taxi data with basic metadata")
def bronze_taxi_trips(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Materialize the bronze layer for yellow taxi data"""
    spark: SparkSession = context.resources.spark

    ingested_at = datetime.now(UTC)

    batch_id = context.run_id
    context.log.info("Starting bronze ingestion for batch '%s'", batch_id)

    result = run_bronze_stage(spark, batch_id=batch_id, ingested_at=ingested_at)
    context.log.info("Wrote '%s' bronze rows to '%s'", result.row_count, result.output_path)

    return dg.MaterializeResult(
        metadata={
            "row_count": result.row_count,
            "batch_id": result.batch_id,
            "ingested_at": result.ingested_at.isoformat(),
            "input_file": dg.MetadataValue.path(str(result.input_path)),
            "output_dataset": dg.MetadataValue.path(str(result.output_path)),
        }
    )
