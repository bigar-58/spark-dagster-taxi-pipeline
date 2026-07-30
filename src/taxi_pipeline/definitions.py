import dagster as dg

from taxi_pipeline.assets.bronze import bronze_taxi_trips
from taxi_pipeline.jobs import taxi_pipeline_job
from taxi_pipeline.resources.spark import spark_resource


defs = dg.Definitions(
    assets=[bronze_taxi_trips],
    jobs=[taxi_pipeline_job],
    resources={"spark": spark_resource},
)
