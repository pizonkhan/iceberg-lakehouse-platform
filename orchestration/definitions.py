"""Dagster entry point: `uv run dagster dev -m orchestration.definitions` from the
repo root (see `make orchestrate`).

Combines the dbt-generated software-defined assets (orchestration/assets/dbt_assets.py,
one per transform/lakehouse model) with the bronze ingestion asset
(orchestration/assets/bronze.py) into one asset graph, and supplies the DbtCliResource
every dbt asset needs to actually run `dbt build` against the live stack.
"""

from __future__ import annotations

import dagster as dg
from dagster_dbt import DbtCliResource

from orchestration.assets.bronze import bronze_ingestion
from orchestration.assets.dbt_assets import lakehouse_dbt_assets
from orchestration.project import lakehouse_project

defs = dg.Definitions(
    assets=[bronze_ingestion, lakehouse_dbt_assets],
    resources={
        "dbt": DbtCliResource(project_dir=lakehouse_project),
    },
)
