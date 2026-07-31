import dagster as dg

from taxi_pipeline.run_gold import run_gold_stage

@dg.multi_asset(
    outs={
        "gold_daily_zone_metrics": dg.AssetOut(),
        "gold_hourly_demand_metrics": dg.AssetOut()
    },
    deps=[dg.AssetKey("silver_valid_taxi_trips")],
    group_name="gold",
    required_resource_keys={"spark"},
    description="Enrich silver taxi data with zone data and create daily and hourly metrics"
)
def gold_taxi_metrics(context: dg.AssetExecutionContext):
    """Materialize both gold analytics datasets"""
    
    context.log.info("starting gold taxi metrics materialization")
    
    result = run_gold_stage(context.resources.spark)
    
    date_from = result.date_from.isoformat() if result.date_from else "not available"
    date_to = result.date_to.isoformat() if result.date_to else "not available"
    
    context.log.info("gold materialization created '%s' daily-zone rows and '%s' hourly-demand rows", result.daily_zone_row_count, result.hourly_demand_row_count)
    
    yield dg.MaterializeResult(
        asset_key="gold_daily_zone_metrics",
        metadata={
            "valid_trip_count": result.valid_trip_count,
            "daily_zone_row_count": result.daily_zone_row_count,
            "unmatched_zone_count": result.unmatched_zone_count,
            "zone_lookup_match_rate": result.zone_lookup_match_rate,
            "date_from": date_from,
            "date_to": date_to,
            "silver_input_dataset": dg.MetadataValue.path(str(result.silver_input_path)),
            "zone_lookup_file": dg.MetadataValue.path(str(result.zone_lookup_input_path)),
            "output_dataset": dg.MetadataValue.path(str(result.daily_zone_output_path)),
            "partition_columns": ("pickup_year, pickup_month")
        },
    )
    
    yield dg.MaterializeResult(
            asset_key="gold_hourly_demand_metrics",
            metadata={
                "valid_trip_count": result.valid_trip_count,
                "hourly_demand_row_count": result.hourly_demand_row_count,
                "unmatched_zone_count": result.unmatched_zone_count,
                "zone_lookup_match_rate": result.zone_lookup_match_rate,
                "date_from": date_from,
                "date_to": date_to,
                "silver_input_dataset": dg.MetadataValue.path(str(result.silver_input_path)),
                "zone_lookup_file": dg.MetadataValue.path(str(result.zone_lookup_input_path)),
                "output_dataset": dg.MetadataValue.path(str(result.hourly_demand_output_path)),
                "partition_columns": ("pickup_year, pickup_month")
            },
        )