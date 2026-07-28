from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame


# TO-DO: Update this to instead move to partition-level replacement instead of full-overwrite.
def overwrite_parquet(df: DataFrame, output_path: Path) -> None:
    """Reset local parquet data to input AKA current data frame"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(output_path))
