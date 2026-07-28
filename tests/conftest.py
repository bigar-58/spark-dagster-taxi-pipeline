from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from taxi_pipeline.spark import create_spark_session


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Create a single Spark session for all tests to reference"""
    session = create_spark_session("taxi-pipeline-tests")

    yield session

    session.stop()
