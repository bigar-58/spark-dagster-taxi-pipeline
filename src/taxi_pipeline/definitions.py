import dagster as dg

from taxi_pipeline.assets.bronze import bronze_taxi_trips
from taxi_pipeline.assets.silver import silver_taxi_trips
from taxi_pipeline.assets.gold import gold_taxi_metrics
from taxi_pipeline.jobs import taxi_pipeline_job
from taxi_pipeline.resources.spark import spark_resource


defs = dg.Definitions(
    assets=[bronze_taxi_trips, silver_taxi_trips, gold_taxi_metrics],
    jobs=[taxi_pipeline_job],
    resources={"spark": spark_resource}
)
