from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from taxi_pipeline.paths import TAXI_ZONE_SAMPLE_FILE
from taxi_pipeline.run_taxi_zones import (
    run_taxi_zone_stage,
)
from taxi_pipeline.sources.taxi_zones import (
    TaxiZoneInput,
    ensure_taxi_zone_input_available,
    resolve_taxi_zone_input
)


def test_zone_source_resolves_to_official_csv() -> None:
    result = resolve_taxi_zone_input()

    assert result.path.name == "taxi_zone_lookup.csv"
    assert result.source_url is not None
    assert result.source_url.endswith("/misc/taxi_zone_lookup.csv")


def test_existing_zone_source_is_reused(tmp_path: Path) -> None:
    input_path = tmp_path / "taxi_zone_lookup.csv"
    input_path.write_text("LocationID,Borough,Zone,service_zone\n",encoding="utf-8")

    input_spec = TaxiZoneInput(path=input_path, source_url="https://example.invalid/zones.csv")

    def unexpected_downloader(url: str, destination: Path) -> None:
        raise AssertionError("Cached lookup should not be downloaded.")

    result = ensure_taxi_zone_input_available(input_spec,downloader=unexpected_downloader)

    assert result == input_spec


def test_run_taxi_zone_stage_writes_normalized_reference(spark: SparkSession,tmp_path: Path) -> None:
    output_path = tmp_path / "taxi-zone-reference"

    result = run_taxi_zone_stage(
        spark,
        input_spec=TaxiZoneInput(path=TAXI_ZONE_SAMPLE_FILE,source_url=None),
        output_path=output_path
    )

    written_df = spark.read.parquet(str(output_path))

    assert result.row_count == 7
    assert result.input_path == TAXI_ZONE_SAMPLE_FILE
    assert result.output_path == output_path

    assert written_df.count() == 7
    assert written_df.columns == [
        "location_id",
        "borough",
        "zone",
        "service_zone",
        "_source_file_path"
    ]

    assert written_df.select("location_id").distinct().count() == 7