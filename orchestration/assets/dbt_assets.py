"""Software-defined assets for every model in transform/lakehouse, generated from
dbt's own manifest.json rather than a hand-built lineage representation.

Asset keys, upstream/downstream dependencies and column schemas all come straight
from the manifest (dagster_dbt.dbt_assets). Column-level lineage is attached at
materialization time via DbtCliInvocation.fetch_column_metadata(), which queries the
warehouse for the built model's and its upstream refs' column schemas and derives
column-to-column lineage from the compiled SQL with sqlglot: see
docs/evidence/dagster/ for a captured example against dim_subscriber.

Excludes the elementary dbt package's own models (test result history, alerting
views, etc, see transform/lakehouse/packages.yml): those are dbt-observability
plumbing bundled by a package dependency, not part of this project's own medallion
model, and are not part of the lineage graph this work package is asked to surface.
"""

# No `from __future__ import annotations` in this module: dagster 1.13's
# _validate_context_type_hint reads the `context` parameter's annotation via plain
# inspect.signature() (not typing.get_type_hints()), so PEP 563 deferred evaluation
# turns the annotation into the literal string "AssetExecutionContext" and dagster's
# `in [AssetExecutionContext, ...]` identity check then always fails. See
# .notes/surprises.md.
from collections.abc import Iterator, Mapping
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from orchestration.project import lakehouse_project


# transform/lakehouse/dbt_project.yml assigns each medallion layer its own schema
# (staging, silver, dimensions, facts) via +schema config, and dagster-dbt already
# prefixes asset keys with that schema (e.g. AssetKey(["silver", "silver_devices"])),
# so the asset graph reads as medallion layers with no extra key mapping needed here.
# The only thing the default DagsterDbtTranslator does not do is group assets in the
# Dagster UI by that same layer, since dbt "groups" (a distinct dbt concept, unused
# in this project) are what it looks at by default, not schema or folder. This
# override reads the model's dbt fqn (its path under models/) instead, so the UI's
# asset graph groups by staging / silver / dimensions / facts, matching CLAUDE.md's
# medallion layer boundaries directly.
class LakehouseDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        fqn = dbt_resource_props.get("fqn", [])
        if len(fqn) >= 3 and fqn[1] == "marts":
            return str(fqn[2])  # dimensions | facts
        if len(fqn) >= 2 and fqn[1] == "intermediate":
            return "silver"  # dbt_project.yml's folder name, this project's own term is silver
        if len(fqn) >= 2:
            return str(fqn[1])  # staging
        return super().get_group_name(dbt_resource_props)


DbtAssetEvent = (
    dg.Output[None] | dg.AssetMaterialization | dg.AssetObservation | dg.AssetCheckResult
)


@dbt_assets(
    manifest=lakehouse_project.manifest_path,
    project=lakehouse_project,
    # AND selection (comma = intersection in dbt selection syntax): every model in
    # the lakehouse package, excluding elementary's models and any non-model
    # resource type the manifest happens to carry.
    select="resource_type:model,package:lakehouse",
    dagster_dbt_translator=LakehouseDbtTranslator(),
)
def lakehouse_dbt_assets(
    context: AssetExecutionContext, dbt: DbtCliResource
) -> Iterator[DbtAssetEvent]:
    """Runs `dbt build` for whichever models/tests were selected for this run.

    `dbt build` (not `dbt run`) so dbt's own data tests execute as part of the same
    invocation and surface as Dagster asset checks (DagsterDbtTranslatorSettings'
    enable_asset_checks defaults to True), matching what `make build` already runs.
    fetch_column_metadata(with_column_lineage=True) queries the warehouse for column
    schemas of each built model and its upstream refs and derives column lineage via
    sqlglot; this only issues lightweight DESCRIBE-style metadata queries, not a
    data scan, so it stays cheap even for the largest models in this project.
    """
    yield from (
        dbt.cli(["build"], context=context).stream().fetch_column_metadata(with_column_lineage=True)
    )
