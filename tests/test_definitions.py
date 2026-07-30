from __future__ import annotations

import dagster as dg

from taxi_pipeline.definitions import defs


def test_dagster_definitions_are_loadable() -> None:
    dg.Definitions.validate_loadable(defs)
