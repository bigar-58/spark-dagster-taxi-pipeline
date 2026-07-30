import dagster as dg

from taxi_pipeline.spark import create_spark_session


@dg.resource
def spark_resource(context: dg.InitResourceContext):
    """Create and clean up the Spark session used by pipeline assets."""

    context.log.info("Starting Spark session.")

    session = create_spark_session(app_name="taxi-pipeline-dagster")

    try:
        yield session
    finally:
        context.log.info("Stopping Spark session.")
        session.stop()
