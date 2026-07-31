from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from taxi_pipeline.io.readers import read_taxi_zone_csv
from taxi_pipeline.io.writers import overwrite_parquet
from taxi_pipeline.paths import (
    TAXI_ZONE_REFERENCE_DIR,
    ensure_data_directories
)
from taxi_pipeline.quality.checks import assert_unique_non_null_key
from taxi_pipeline.sources.taxi_zones import (
    TaxiZoneInput,
    ensure_taxi_zone_input_available,
    resolve_taxi_zone_input
)
from taxi_pipeline.spark import create_spark_session
from taxi_pipeline.transforms.gold import build_taxi_zone_dim

@dataclass(frozen=True)
class TaxiZoneRunResult:
    input_path: Path
    output_path: Path
    source_url: str | None
    source_file_size_bytes: int
    row_count: int

def run_taxi_zone_stage(spark: SparkSession, *, input_spec: TaxiZoneInput | None = None, output_path: Path = TAXI_ZONE_REFERENCE_DIR):
    """Download, normalize and validate taxi zones"""
    
    ensure_data_directories()
    
    resolved_input = input_spec if input_spec else resolve_taxi_zone_input()
    
    available_input = ensure_taxi_zone_input_available(resolved_input)
    
    raw_zones_df = read_taxi_zone_csv(spark=spark, input_path=available_input.path)
    taxi_zones_df = build_taxi_zone_dim(raw_zone_df=raw_zones_df).persist()
    
    try:
        assert_unique_non_null_key(taxi_zones_df, key_column="location_id", dataset_name="taxi-zone reference")
        
        row_count = taxi_zones_df.count()
        if row_count == 0: 
            raise ValueError("Taxi zone reference dataset cannot be empty")
        
        overwrite_parquet(taxi_zones_df, output_path)
        
    finally:
        taxi_zones_df.unpersist()
        
    return TaxiZoneRunResult(
        input_path=available_input.path,
        output_path=output_path,
        source_url=available_input.source_url,
        source_file_size_bytes=available_input.path.stat().st_size,
        row_count=row_count
    )
    

def main():
    """Materialize the TLC taxi zone reference data"""
    
    spark = create_spark_session("taxi-zone-reference")
    
    try:
        result = run_taxi_zone_stage(spark)
        
        print(f"Wrote {result.row_count} taxi zones to {result.output_path}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()