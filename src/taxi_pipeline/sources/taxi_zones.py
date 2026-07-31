from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from taxi_pipeline.io.downloads import download_file
from taxi_pipeline.paths import TAXI_ZONE_RAW_FILE

TAXI_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

DownloadFunction = Callable[[str, Path], None]


@dataclass(frozen=True)
class TaxiZoneInput:
    """Resolved source information for the taxi-zone lookup."""

    path: Path
    source_url: str | None
    

def resolve_taxi_zone_input() -> TaxiZoneInput:
    """Resolve the official TLC taxi-zone lookup source."""

    return TaxiZoneInput(path=TAXI_ZONE_RAW_FILE,source_url=TAXI_ZONE_LOOKUP_URL)


def ensure_taxi_zone_input_available(input_spec: TaxiZoneInput, *, downloader: DownloadFunction = download_file,) -> TaxiZoneInput:
    """Reuse the cached lookup or download it when absent."""

    if input_spec.path.is_file() and input_spec.path.stat().st_size > 0:
        return input_spec

    if not input_spec.source_url:
        raise FileNotFoundError(f"Taxi-zone input not found: {input_spec.path}")

    downloader(input_spec.source_url,input_spec.path)

    if not input_spec.path.is_file() or input_spec.path.stat().st_size == 0:
        raise RuntimeError("Taxi-zone download did not create a usable file.")

    return input_spec