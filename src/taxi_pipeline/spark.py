from __future__ import annotations

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "taxi-pipeline") -> SparkSession:
    """
    Creates a local spark session and centrally handles any configuration
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    