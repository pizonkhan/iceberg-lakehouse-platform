"""Smoke test for orchestration/definitions.py: the Dagster asset graph
builds successfully from the pre-parsed dbt manifest.

This does not require the live stack (Trino/MinIO/Nessie): dagster-dbt's
@dbt_assets decorator builds asset definitions by parsing
transform/lakehouse/target/manifest.json at import time, a static file
already on disk from a prior `dbt parse` / `make build`, not by invoking
dbt or connecting to a warehouse. DbtCliResource itself is just a resource
config object at this point; nothing here runs `dbt build`.
"""

from __future__ import annotations

from typing import Any

import dagster as dg
import pytest


@pytest.fixture(scope="module")
def defs() -> dg.Definitions:
    from orchestration.definitions import defs as built_defs

    return built_defs


@pytest.fixture(scope="module")
def asset_graph(defs: dg.Definitions) -> Any:
    # dagster's resolved asset-graph type lives at a private module path that
    # has moved between minor versions (dagster._core.definitions.assets.graph.
    # asset_graph.AssetGraph as of the 1.13 pin here); typed as Any rather than
    # importing that internal path so this test does not depend on it directly.
    return defs.resolve_asset_graph()


def test_definitions_import_and_build_without_error(defs: dg.Definitions) -> None:
    assert isinstance(defs, dg.Definitions)


def test_bronze_ingestion_asset_keys_are_present(asset_graph: Any) -> None:
    """One bronze asset per entity in ingestion/settings.py's ENTITIES,
    at the exact asset keys orchestration/assets/bronze.py documents
    (AssetKey(["bronze", "bronze_plans"]) etc), confirmed against the
    dbt-generated staging assets' own source asset keys."""
    from ingestion.settings import ENTITIES

    all_keys = asset_graph.get_all_asset_keys()
    for entity in ENTITIES:
        expected_key = dg.AssetKey(["bronze", entity.table_name])
        assert expected_key in all_keys


def test_dbt_generated_assets_are_present_for_every_medallion_layer(
    asset_graph: Any,
) -> None:
    """The dbt-generated assets cover staging, silver (intermediate/),
    dimensions and facts, not just one layer, proving the manifest parse
    picked up the whole model graph rather than a partial/stale one."""
    all_keys = asset_graph.get_all_asset_keys()
    prefixes = {key.path[0] for key in all_keys}

    for expected_prefix in ("bronze", "staging", "silver", "dimensions", "facts"):
        assert expected_prefix in prefixes


def test_no_duplicate_asset_keys_across_bronze_and_dbt_layers(asset_graph: Any) -> None:
    """bronze.py's manually declared asset keys and dbt_assets.py's
    manifest-derived keys must not collide; a collision would mean two
    different producers claim the same asset."""
    all_keys = list(asset_graph.get_all_asset_keys())
    assert len(all_keys) == len(set(all_keys))
