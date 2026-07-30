from __future__ import annotations

import dagster as dg
from pydantic import PrivateAttr
from pyspark.sql import SparkSession

from taxi_pipeline.spark import create_spark_session


class SparkResource(dg.ConfigurableResource):
    """Provide a local Spark session to dagster"""

    app_name: str = "taxi-pipeline-dagster"
    session: SparkSession | None = PrivateAttr(default=None)

    def setup_for_execution(self, context: dg.InitResourceContext):
        """Create spark session before assets execute"""

        context.log.info("Starting spark session with for  '%s' ", self.app_name)

        self.session = create_spark_session(app_name=self.app_name)

    def teardown_after_execution(self, context):
        """Stop spark session after asset execute"""

        if not self.session:
            return

        context.log.info("Stopping spark session")
        self.session.stop()
        self.session = None

    @property
    def session(self) -> SparkSession:
        """Return current spark session"""

        if not self.session:
            raise RuntimeError("Spark session was accessed before resource initialization")

        return self.session
