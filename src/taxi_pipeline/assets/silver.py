import dagster as dg

from taxi_pipeline.assets.bronze import bronze_taxi_trips
from taxi_pipeline.run_silver import run_silver_stage


@dg.multi_asset(
    outs={
        "silver_valid_taxi_trips": dg.AssetOut(),
        "silver_invalid_taxi_trips": dg.AssetOut()
    },
    deps=[bronze_taxi_trips],
    group_name="silver",
    required_resource_keys={"spark"},
    description="Transform bronze layer into valid and invalid silver data"
)
def silver_taxi_trips(context: dg.AssetExecutionContext):
    """Materializes the invalid + valid silver datasets in single pass"""
    
    context.log.info("Starting silver transformation")
    
    result = run_silver_stage(context.resources.spark)
    
    context.log.info("Silver transformation produced '%s' valid and '%s' invalid rows", result.valid_row_count, result.invalid_row_count)
    
    yield dg.MaterializeResult(
        asset_key="silver_valid_taxi_trips",
        metadata={
            "total_input_rows": result.total_row_count,
            "valid_row_count": result.valid_row_count,
            "output_dataset": dg.MetadataValue.path(str(result.valid_output_path)),
            "partition_columns": "pickup_year, pickup_month"
        }
    )
    
    yield dg.MaterializeResult(
            asset_key="silver_invalid_taxi_trips",
            metadata={
                "total_input_rows": result.total_row_count,
                "invalid_row_count": result.invalid_row_count,
                "invalid_row_rate": result.invalid_row_rate,
                "output_dataset": dg.MetadataValue.path(str(result.invalid_output_path))
            }
        )
    