"""Bronze ingestion as a Dagster asset: the one layer dagster-dbt does not cover.

dagster-dbt generates assets from transform/lakehouse's dbt manifest, which starts
at the staging models. Those models read from dbt sources declared in
transform/lakehouse/models/staging/sources.yml (the `bronze` source, one table per
entity), and dagster-dbt already computes an asset key for each of those sources
(confirmed directly against the manifest: AssetKey(["bronze", "bronze_plans"]), etc,
one per table in sources.yml). This module supplies the actual upstream asset at
each of those exact keys, so the graph is contiguous end to end: bronze ingestion
feeds staging feeds silver feeds marts, all as real Dagster assets, not a dangling
external dependency stub.

Wraps ingestion/pipeline.py's real entrypoint directly (the same function `uv run
python -m ingestion.pipeline` calls, which `make seed` already uses) rather than
reimplementing or shelling out to it. One dlt pipeline.run() call loads every
entity in a single pass, so this is a single multi_asset with one output per bronze
table (can_subset=False: the underlying pipeline call is not currently able to load
a subset of entities from a Dagster-level partial selection without re-parsing that
selection back into entity names, which would duplicate logic pipeline.py's own CLI
already owns; loading everything each run matches how `make seed` is actually used
in this project today).

The synthetic data generator (generation/) is deliberately not wrapped as a Dagster
asset. It is a one-time, seed-driven batch step that produces the fixed input
dataset (generation/output/) this entire project's bronze/silver/gold layers are
built and tested against; every model in transform/lakehouse and every number in
.notes/decisions.md assumes that exact dataset. Making it a re-runnable Dagster
asset upstream of bronze would invite regenerating it (a new random draw even with
the same seed strategy reused differently, or a config change) and silently
invalidating that whole chain. It is a manual, explicit step
(`uv run python -m generation.generate`), not part of this orchestration graph.
"""

# No `from __future__ import annotations` in this module: see dbt_assets.py's
# top-of-file comment and .notes/surprises.md, the `context` parameter's annotation
# has to resolve to the actual AssetExecutionContext class object at decoration
# time, not a deferred string.
from collections.abc import Iterator

import dagster as dg
from dagster import AssetExecutionContext

from ingestion.pipeline import run as run_bronze_ingestion
from ingestion.settings import ENTITIES

_BRONZE_OUTS = {
    entity.table_name: dg.AssetOut(
        key=dg.AssetKey(["bronze", entity.table_name]),
        description=(
            f"Bronze Iceberg table iceberg.bronze.{entity.table_name}: raw, "
            f"append-only, loaded by dlt from generation/output/{entity.directory}/. "
            "Source columns verbatim plus ingestion metadata "
            "(_source_file, _ingested_at, _batch_id, _payload_hash). Never modified "
            "after load, see CLAUDE.md's medallion layer boundaries."
        ),
        group_name="bronze",
    )
    for entity in ENTITIES
}


@dg.multi_asset(
    name="bronze_ingestion",
    outs=_BRONZE_OUTS,
    can_subset=False,
    compute_kind="dlt",
    description=(
        "Runs ingestion/pipeline.py's run() (the same entrypoint `uv run python -m "
        "ingestion.pipeline` and `make seed` use) for every source entity, loading "
        "generation/output/ parquet into one Iceberg table per entity in the bronze "
        "namespace. The dbt-generated staging assets read these tables through "
        "transform/lakehouse/models/staging/sources.yml, which is what makes this "
        "asset their upstream dependency in the graph."
    ),
)
def bronze_ingestion(context: AssetExecutionContext) -> Iterator[dg.MaterializeResult[None]]:
    run_bronze_ingestion()
    for entity in ENTITIES:
        yield dg.MaterializeResult(
            asset_key=dg.AssetKey(["bronze", entity.table_name]),
            metadata={"dlt_entity": entity.table_name, "source_directory": entity.directory},
        )
