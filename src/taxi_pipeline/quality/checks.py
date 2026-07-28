from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_unique_non_null_key(df: DataFrame, *, key_column: str, dataset_name: str) -> None:
    """
    General utility that validates whether inputted lookup key is missing, null, or duplicated
    AKA validates whether a key is valid
    """

    if key_column not in df.columns:
        raise ValueError(f"{dataset_name} is missing key column: {key_column}")

    key_counts = df.groupBy(key_column).count()

    has_null_key = key_counts.filter(F.col(key_column).isNull()).limit(1).count() > 0

    if has_null_key:
        raise ValueError(f"{dataset_name} contains a null {key_column}")

    has_duplicate_key = key_counts.filter(F.col("count") > 1).limit(1).count() > 0

    if has_duplicate_key:
        raise ValueError(f"{dataset_name} has duplicate {key_column} values")
