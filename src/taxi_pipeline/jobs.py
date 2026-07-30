from __future__ import annotations

import dagster as dg


taxi_pipeline_job = dg.define_asset_job(
    name="taxi_pipeline_job",
    selection=dg.AssetSelection.all(),
    executor_def=dg.in_process_executor,
    description="Materialize the complete NYC taxi analytics pipeline.",
)
