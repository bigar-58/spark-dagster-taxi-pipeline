import dagster as dg
from pyspark.sql import SparkSession

from taxi_pipeline.run_taxi_zones import run_taxi_zone_stage


@dg.asset(
    group_name="reference",
    required_resource_keys={"spark"},
    description=("Validated TLC taxi-zone lookup used to enrich pickup location IDs.")
)
def taxi_zone_lookup(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Materialize the official TLC taxi-zone reference."""

    spark: SparkSession = context.resources.spark

    context.log.info("Starting taxi-zone reference materialization.")
    
    result = run_taxi_zone_stage(spark)

    context.log.info("Wrote '%s' taxi zones to '%s'.", result.row_count, result.output_path)

    metadata = {
        "row_count": result.row_count,
        "source_file_size_bytes": result.source_file_size_bytes,
        "input_file": dg.MetadataValue.path(str(result.input_path)),
        "output_dataset": dg.MetadataValue.path(str(result.output_path)),
    }

    if result.source_url is not None:
        metadata["source_url"] = dg.MetadataValue.url(result.source_url)

    return dg.MaterializeResult(metadata=metadata)