from __future__ import annotations

from taxi_pipeline.paths import (
    BRONZE_DIR,
    DATA_DIR,
    GOLD_DIR,
    RAW_DIR,
    REPORTS_DIR,
    SAMPLE_DIR,
    SILVER_DIR,
    TAXI_ZONES_SAMPLE_DIR,
    YELLOW_TAXI_SAMPLE_DIR,
)


def test_project_data_directories_are_under_data_dir() -> None:
    expected_directories = [RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, REPORTS_DIR, SAMPLE_DIR]

    for directory in expected_directories:
        assert directory.parent == DATA_DIR


def test_sample_input_directories_are_under_sample_dir() -> None:
    assert YELLOW_TAXI_SAMPLE_DIR.parent == SAMPLE_DIR
    assert TAXI_ZONES_SAMPLE_DIR.parent == SAMPLE_DIR
