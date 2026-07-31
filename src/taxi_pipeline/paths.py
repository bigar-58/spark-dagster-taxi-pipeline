from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
REFERENCE_DIR = DATA_DIR / "reference"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
REPORTS_DIR = DATA_DIR / "reports"
SAMPLE_DIR = DATA_DIR / "sample"

YELLOW_TAXI_SAMPLE_DIR = SAMPLE_DIR / "yellow_taxi"
TAXI_ZONES_SAMPLE_DIR = SAMPLE_DIR / "taxi_zones"
YELLOW_TAXI_SAMPLE_FILE = YELLOW_TAXI_SAMPLE_DIR / "yellow_tripdata_sample.csv"

TAXI_ZONE_RAW_DIR = RAW_DIR / "taxi_zones"
TAXI_ZONE_RAW_FILE = TAXI_ZONE_RAW_DIR / "taxi_zone_lookup.csv"
TAXI_ZONE_REFERENCE_DIR = REFERENCE_DIR / "taxi_zones"

YELLOW_TAXI_RAW_DIR = RAW_DIR / "yellow_taxi"

YELLOW_TAXI_BRONZE_DIR = BRONZE_DIR / "yellow_taxi_trips"

YELLOW_TAXI_SILVER_DIR = SILVER_DIR / "yellow_taxi_trips"
YELLOW_TAXI_SILVER_VALID_DIR = YELLOW_TAXI_SILVER_DIR / "valid"
YELLOW_TAXI_SILVER_INVALID_DIR = YELLOW_TAXI_SILVER_DIR / "invalid"

TAXI_ZONE_SAMPLE_FILE = TAXI_ZONES_SAMPLE_DIR / "taxi_zone_lookup_sample.csv"
YELLOW_TAXI_GOLD_DIR = GOLD_DIR / "yellow_taxi"
DAILY_ZONE_METRICS_GOLD_DIR = YELLOW_TAXI_GOLD_DIR / "daily_zone_metrics"
HOURLY_DEMAND_METRICS_GOLD_DIR = YELLOW_TAXI_GOLD_DIR / "hourly_demand_metrics"


def ensure_data_directories():
    """
    Centralized function to call to create/organize local directories for data used by the pipeline
    """
    directories_to_init = [
    RAW_DIR,
    REFERENCE_DIR,
    TAXI_ZONE_RAW_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
    REPORTS_DIR,
    YELLOW_TAXI_BRONZE_DIR,
    YELLOW_TAXI_SILVER_VALID_DIR,
    YELLOW_TAXI_SILVER_INVALID_DIR,
    TAXI_ZONE_REFERENCE_DIR,
    DAILY_ZONE_METRICS_GOLD_DIR,
    HOURLY_DEMAND_METRICS_GOLD_DIR
    ]
    for directory in directories_to_init:
        directory.mkdir(parents=True, exist_ok=True)
