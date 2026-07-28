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


def ensure_data_directories():
    """
    Centralized function to call to create/organize local directories for data used by the pipeline
    """
    for directory in [RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, REPORTS_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
