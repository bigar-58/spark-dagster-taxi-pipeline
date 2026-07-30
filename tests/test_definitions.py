from __future__ import annotations

import dagster as dg

from taxi_pipeline.definitions import defs


def test_dagster_definitions_object_exists() -> None:
    """Really simple validation to make sure we don't have basic errors in definitions.py"""
    assert isinstance(defs, dg.Definitions)
