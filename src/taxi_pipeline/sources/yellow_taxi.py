from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.error import URLError
from urllib.request import Request, urlopen

from taxi_pipeline.paths import (
    YELLOW_TAXI_RAW_DIR,
    YELLOW_TAXI_SAMPLE_FILE
)

YellowTaxiSource = Literal["sample", "tlc"]
YellowTaxiInputFormat = Literal["csv", "parquet"]

TLC_YELLOW_TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
SUPPORTED_TLC_YEAR = 2024 # TO-DO Expand to other years, but need to review schema evolution first. 

DownloadFunction = Callable[[str, Path], None]

# Default input source will be through local sample file for basic run example

@dataclass(frozen=True)
class YellowTaxiInput:
    """Resolved source information for single Bronze ingestion"""
    
    path: Path
    input_format: YellowTaxiInputFormat
    source: YellowTaxiSource
    year: int | None = None
    month: int | None = None
    source_url: str | None = None
    

def resolve_yellow_taxi_input(
    *,
    source: str = "sample",
    year: int = SUPPORTED_TLC_YEAR,
    month: int = 1
) -> YellowTaxiInput:
    """
    Resolve input TLC config into standardized input
    """
    
    if source == "sample":
        return YellowTaxiInput(
            path=YELLOW_TAXI_SAMPLE_FILE,
            input_format='csv',
            source="sample"
        )
    
    if source != "tlc":
        raise ValueError("source must be one of either 'sample' or 'tlc'")
    
    if year != SUPPORTED_TLC_YEAR:
        raise ValueError(f"Only {SUPPORTED_TLC_YEAR} TLC data is currently supported.")

    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12.")

    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    
    return YellowTaxiInput(
        path=YELLOW_TAXI_RAW_DIR / filename,
        input_format="parquet",
        source="tlc",
        year=year,
        month=month,
        source_url=f"{TLC_YELLOW_TAXI_BASE_URL}/{filename}"
    )


def download_file(url: str, destination: Path) -> None:
    """Download remote file w/o exposing partial final file"""
    
    destination.parent.mkdir(parents=True, exist_ok=True)
    
    temporary_path = destination.with_suffix(f"{destination.suffix}.part")
    
    request = Request(
        url,
        headers={"User-Agent": "spark-dagster-taxi-pipeline/1.0"}
    )
    
    try: 
        with(
            urlopen(request, timeout=60) as response, 
            temporary_path.open("wb") as output_file
        ):
            shutil.copyfileobj(response, output_file, length=1024*1024)
            
        if temporary_path.stat().st_size == 0:
            raise OSError("Downloaded file was empty")
        
        temporary_path.replace(destination)
        
    except (OSError, URLError) as exc:
        temporary_path.unlink(missing_ok=True)
        
        raise RuntimeError(f"unable to download TLC data from {url}") from exc
    
    
def ensure_yellow_taxi_input_available(input_spec: YellowTaxiInput, *, downloader: DownloadFunction = download_file) -> YellowTaxiInput:
    """Reuse a cached source file or download it when required"""
    
    if input_spec.path.is_file() and input_spec.path.stat().st_size > 0:
        return input_spec
    
    if input_spec.source == "sample":
        raise FileNotFoundError(f"Sample taxi input not foud: {input_spec.path}")
    
    if input_spec.source_url is None:
        raise ValueError("TLC input must include source url")
    
    downloader(input_spec.source_url, input_spec.path)
    
    if not input_spec.path.is_file() or input_spec.path.stat().st_size == 0:
        raise RuntimeError("TLC download did not create a usable file")
    
    return input_spec