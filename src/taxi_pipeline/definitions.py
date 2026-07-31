import dagster as dg

from taxi_pipeline.assets.taxi_zones import taxi_zone_lookup
from taxi_pipeline.assets.bronze import bronze_taxi_trips
from taxi_pipeline.assets.silver import silver_taxi_trips
from taxi_pipeline.assets.gold import gold_taxi_metrics
from taxi_pipeline.assets.postgres import postgres_gold_marts
from taxi_pipeline.jobs import taxi_pipeline_job
from taxi_pipeline.resources.spark import spark_resource
from taxi_pipeline.resources.postgres import postgres_settings_resource


defs = dg.Definitions(
    assets=[taxi_zone_lookup, bronze_taxi_trips, silver_taxi_trips, gold_taxi_metrics, postgres_gold_marts],
    jobs=[taxi_pipeline_job],
    resources={
        "spark": spark_resource,
        "postgres_settings": postgres_settings_resource
    }
)
