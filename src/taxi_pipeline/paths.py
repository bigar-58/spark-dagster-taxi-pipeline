from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
REPORTS_DIR = DATA_DIR / "reports"
SAMPLE_DIR = DATA_DIR / "sample"

YELLOW_TAXI_SAMPLE_DIR = SAMPLE_DIR / "yellow_taxi"
TAXI_ZONES_SAMPLE_DIR = SAMPLE_DIR / "taxi_zones"
YELLOW_TAXI_SAMPLE_FILE = YELLOW_TAXI_SAMPLE_DIR / "yellow_tripdata_sample.csv"

YELLOW_TAXI_BRONZE_DIR = BRONZE_DIR / "yellow_taxi_trips"

YELLOW_TAXI_SILVER_DIR = SILVER_DIR / "yellow_taxi_trips"
YELLOW_TAXI_SILVER_VALID_DIR = YELLOW_TAXI_SILVER_DIR / "valid"
YELLOW_TAXI_SILVER_INVALID_DIR = YELLOW_TAXI_SILVER_DIR / "invalid"


def ensure_data_directories():
    """
    Centralized function to call to create/organize local directories for data used by the pipeline
    """
    directories_to_init = [
        RAW_DIR,
        BRONZE_DIR,
        SILVER_DIR,
        GOLD_DIR,
        REPORTS_DIR,
        YELLOW_TAXI_BRONZE_DIR,
        YELLOW_TAXI_SILVER_DIR,
        YELLOW_TAXI_SILVER_VALID_DIR,
        YELLOW_TAXI_SILVER_INVALID_DIR,
    ]
    for directory in directories_to_init:
        directory.mkdir(parents=True, exist_ok=True)
