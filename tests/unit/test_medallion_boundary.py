"""Mechanically enforces CLAUDE.md's medallion layer boundary: "A gold model reading
directly from bronze is an architecture violation and gets rejected, no exceptions."

Before this test existed, that rule was enforced only by human/agent review at build
time, with no automated check catching a future violation. Reads the static dbt
manifest (transform/lakehouse/target/manifest.json, produced by `make build` or a
bare `dbt parse`), the same pre-parsed artifact test_orchestration_definitions.py
already relies on, so this does not require the live stack either.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = REPO_ROOT / "transform" / "lakehouse" / "target" / "manifest.json"

# staging/ is the one layer CLAUDE.md and sources.yml both name as the sanctioned
# place bronze is read from directly ("staging models are the only place these
# sources are read from directly", transform/lakehouse/models/staging/sources.yml).
# intermediate/ (silver) and marts/ (gold: dimensions, facts, bridge) must reach
# bronze data only transitively, through a staging/silver ref(), never a direct
# source('bronze', ...).
LAYERS_ALLOWED_TO_TOUCH_BRONZE_DIRECTLY = {"staging"}


@pytest.fixture(scope="module")
def manifest() -> dict[str, Any]:
    if not MANIFEST_PATH.exists():
        pytest.skip(
            f"{MANIFEST_PATH} does not exist yet; run `make build` (or `dbt parse`) "
            "against the trino target first to produce it."
        )
    result: dict[str, Any] = json.loads(MANIFEST_PATH.read_text())
    return result


def _layer_of(node: dict[str, Any]) -> str | None:
    """First path segment under models/, e.g. 'staging', 'intermediate', 'marts'."""
    path = node.get("path", "")
    parts = Path(path).parts
    return parts[0] if parts else None


def test_manifest_has_bronze_sources_and_medallion_layers(manifest: dict[str, Any]) -> None:
    """Sanity check on the fixture itself: fail loudly if the manifest is stale or
    from an unrelated project, rather than the real assertions below silently
    passing over zero nodes."""
    bronze_sources = [
        key for key in manifest["sources"] if manifest["sources"][key]["source_name"] == "bronze"
    ]
    assert len(bronze_sources) == 9, (
        "expected 9 bronze source tables, manifest looks stale or wrong"
    )

    layers = {
        _layer_of(node)
        for node in manifest["nodes"].values()
        if node.get("resource_type") == "model"
    }
    for expected_layer in ("staging", "intermediate", "marts"):
        assert expected_layer in layers


def test_no_gold_or_silver_model_reads_bronze_directly(manifest: dict[str, Any]) -> None:
    """The actual architecture boundary check: no intermediate/ (silver) or marts/
    (gold: dimensions, facts, bridge) model has a direct dependency edge on a
    bronze source. A staging model may ref() such a model, and that is exactly
    how bronze data reaches silver and gold correctly, layered rather than
    skipped; this only fails on a *direct* source('bronze', ...) reference from
    the wrong layer.
    """
    violations: list[str] = []
    for unique_id, node in manifest["nodes"].items():
        if node.get("resource_type") != "model":
            continue
        layer = _layer_of(node)
        if layer is None or layer in LAYERS_ALLOWED_TO_TOUCH_BRONZE_DIRECTLY:
            continue

        for dep_id in node.get("depends_on", {}).get("nodes", []):
            if dep_id.startswith("source.") and ".bronze." in dep_id:
                violations.append(f"{unique_id} (layer={layer}) directly depends on {dep_id}")

    assert not violations, (
        "medallion boundary violation, a gold or silver model reads bronze directly:\n"
        + "\n".join(violations)
    )
