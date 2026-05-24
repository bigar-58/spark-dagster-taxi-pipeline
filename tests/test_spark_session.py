from __future__ import annotations

from taxi_pipeline.spark import create_spark_session


def test_create_spark_session_can_run_simple_datafram_job() -> None:
    
    session = create_spark_session("taxi-pipeline-test")
    
    try:
        df = session.createDataFrame(
            [
                ("2024-01-01", 10),
                ("2024-01-02", 15)
            ],
            ["pickup_date", "trip_count"]
        )
        
        result = df.groupBy().sum("trip_count").collect()[0][0]
        
        assert result == 25
    finally: 
        session.stop()
        