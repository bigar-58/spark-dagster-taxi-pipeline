from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

YELLOW_TAXI_RAW_COLUMNS: tuple[str, ...] = (
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
)

# Note: For the Bronze layer, data should be as unstructured as possible -> ingest all data as a string for later casting in Silver
YELLOW_TAXI_RAW_SCHEMA = StructType(
    [
        StructField(column_name, StringType(), nullable=True)
        for column_name in YELLOW_TAXI_RAW_COLUMNS
    ]
)
