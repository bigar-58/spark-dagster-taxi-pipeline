from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_bronze_taxi_trips(raw_df: DataFrame, *, batch_id: str, ingested_at: datetime) -> DataFrame:
    """Ensure that we have some basic traceability fields added to ingested taxi-data"""
    if not batch_id.strip():
        raise ValueError("batch_id cannot be null/blank")

    if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
        raise ValueError("ingested_at must be timezone-aware")

    if "_source_file_path" not in raw_df.columns:
        raise ValueError("raw_df must include _source_file_path")

    return raw_df.withColumn("_batch_id", F.lit(batch_id)).withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
