from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame


# TO-DO: Update this to instead move to partition-level replacement instead of full-overwrite.
def overwrite_parquet(df: DataFrame, output_path: Path) -> None:
    """Reset local parquet data to input AKA current data frame"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(output_path))


def overwrite_partitioned_parquet(
    df: DataFrame, output_path: Path, partition_columns: Sequence[str]
) -> None:
    """Replace a parquet dataset partitioned by the input columns"""
    if not partition_columns:
        raise ValueError("At least one partition column is required")

    missing_columns = sorted(set(partition_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(
            f"There exists partition column(s) missing from DataFrame: {missing_columns}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # partitioned write based on requested columns
    df.write.mode("overwrite").partitionBy(*partition_columns).parquet(str(output_path))
