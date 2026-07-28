from __future__ import annotations

from pyspark.sql import SparkSession


def test_spark_can_run_simple_dataframe_job(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            ("2024-01-01", 10),
            ("2024-01-02", 15),
        ],
        ["pickup_date", "trip_count"],
    )

    result = df.groupBy().sum("trip_count").collect()[0][0]

    assert result == 25
