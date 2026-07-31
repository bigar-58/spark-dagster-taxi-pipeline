from dataclasses import replace
from pathlib import Path

import pytest

from taxi_pipeline.paths import (
    YELLOW_TAXI_RAW_DIR,
    YELLOW_TAXI_SAMPLE_FILE
)
from taxi_pipeline.sources.yellow_taxi import (
    ensure_yellow_taxi_input_available,
    resolve_yellow_taxi_input
)


def test_sample_source_resolves_to_committed_csv() -> None:
    result = resolve_yellow_taxi_input()

    assert result.source == "sample"
    assert result.input_format == "csv"
    assert result.path == YELLOW_TAXI_SAMPLE_FILE
    assert result.year is None
    assert result.month is None
    assert result.source_url is None


def test_tlc_source_resolves_to_monthly_parquet() -> None:
    result = resolve_yellow_taxi_input(source="tlc",year=2024,month=1)

    assert result.source == "tlc"
    assert result.input_format == "parquet"
    assert result.year == 2024
    assert result.month == 1
    assert result.path == YELLOW_TAXI_RAW_DIR / "yellow_tripdata_2024-01.parquet"
    assert result.source_url is not None
    assert result.source_url.endswith("yellow_tripdata_2024-01.parquet")


@pytest.mark.parametrize("month", [0, 13])
def test_tlc_source_rejects_invalid_month(month: int) -> None:
    with pytest.raises(ValueError,match="between 1 and 12"):
        resolve_yellow_taxi_input(source="tlc",year=2024,month=month,)


def test_tlc_source_rejects_unsupported_year() -> None:
    with pytest.raises(ValueError,match="Only 2024"):
        resolve_yellow_taxi_input(
            source="tlc",
            year=2025,
            month=1,
        )


def test_missing_tlc_source_is_downloaded(tmp_path: Path) -> None:
    resolved = resolve_yellow_taxi_input(
        source="tlc",
        year=2024,
        month=1
    )
    test_input = replace(resolved,path=tmp_path / resolved.path.name)

    calls: list[tuple[str, Path]] = []

    def fake_downloader(url: str,destination: Path,) -> None:
        calls.append((url, destination))
        destination.parent.mkdir(parents=True,exist_ok=True)
        destination.write_bytes(b"test-parquet")

    result = ensure_yellow_taxi_input_available(test_input,downloader=fake_downloader)

    assert result == test_input
    assert calls == [
        (test_input.source_url,test_input.path)
    ]
    assert test_input.path.read_bytes() == b"test-parquet"


def test_existing_tlc_source_is_reused(tmp_path: Path) -> None:
    resolved = resolve_yellow_taxi_input(
        source="tlc",
        year=2024,
        month=1
    )
    test_input = replace(resolved,path=tmp_path / resolved.path.name,)
    test_input.path.write_bytes(b"cached-parquet")

    def unexpected_downloader(url: str,destination: Path) -> None:
        raise AssertionError("Cached input should not be downloaded.")

    result = ensure_yellow_taxi_input_available(test_input,downloader=unexpected_downloader)

    assert result == test_input