import dagster as dg

from taxi_pipeline.run_publish_gold import run_publish_gold_stage

@dg.asset(
    deps=[
        dg.AssetKey("gold_daily_zone_metrics"),
        dg.AssetKey("gold_hourly_demand_metrics")
    ],
    group_name="warehouse",
    required_resource_keys={
        "spark",
        "postgres_settings"
    },
    description= "Transactionally publish both Gold analytics datasets to the Postgres mart schema."
)
def postgres_gold_marts(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Publish both gold marts and log audit row"""
    
    settings = context.resources.postgres_settings
    
    context.log.info("starting transactional gold publication for run_id: '%s'", context.run_id)
    
    result = run_publish_gold_stage(context.resources.spark, settings=settings, run_id=context.run_id)
    
    context.log.info(
        "Published %s daily-zone rows and %s hourly rows for %s through %s.",
        result.daily_zone_row_count,
        result.hourly_demand_row_count,
        result.date_from,
        result.date_to
    )
    
    return dg.MaterializeResult(
        metadata={
            "publish_run_id": result.run_id,
            "date_from": result.date_from.isoformat(),
            "date_to": result.date_to.isoformat(),
            "daily_zone_row_count": result.daily_zone_row_count,
            "hourly_demand_row_count": result.hourly_demand_row_count,
            "daily_zone_input_dataset": dg.MetadataValue.path(str(result.daily_zone_input_path)),
            "hourly_demand_input_dataset": dg.MetadataValue.path(str(result.hourly_demand_input_path)),
            "database": settings.dbname,
            "daily_zone_table": "mart.daily_zone_metrics",
            "hourly_demand_table": "mart.hourly_demand_metrics",
            "audit_table": "audit.gold_publish_runs"
        }
    )