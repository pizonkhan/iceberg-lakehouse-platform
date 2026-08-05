"""Shared dbt project pointer and target selection for the orchestration layer.

Dagster discovers the transform/lakehouse dbt project's models, columns and
dependencies by parsing its manifest.json (dagster_dbt.DbtProject / @dbt_assets),
not from a hand-built lineage representation, so the manifest stays the single
source of truth for the model graph.
"""

from __future__ import annotations

import os
from pathlib import Path

from dagster_dbt import DbtProject

REPO_ROOT = Path(__file__).resolve().parent.parent
LAKEHOUSE_PROJECT_DIR = REPO_ROOT / "transform" / "lakehouse"

# transform/lakehouse/profiles.yml itself defaults DBT_TARGET to "duckdb" (the fast
# local/CI target). Dagster's job is to orchestrate the real deployed stack (Trino /
# Nessie / MinIO, infra/docker-compose.yml), which is what this work package exists
# to prove out, so the default here is "trino" instead. Overridable with
# DAGSTER_DBT_TARGET for anyone who wants `dagster dev` pointed at duckdb locally.
DBT_TARGET = os.environ.get("DAGSTER_DBT_TARGET", "trino")

lakehouse_project = DbtProject(
    project_dir=LAKEHOUSE_PROJECT_DIR,
    profiles_dir=LAKEHOUSE_PROJECT_DIR,
    target=DBT_TARGET,
)

# Deliberately not calling lakehouse_project.prepare_if_dev() here. That helper is
# dagster-dbt's recommended dev-mode convenience: under `dagster dev` it re-runs
# `dbt deps` (a network call to the dbt package hub) and `dbt parse` on every process
# start to pick up model edits automatically. This project already has an
# established, explicit way to regenerate the manifest (`make build`, or a bare
# `dbt parse` from transform/lakehouse), and dbt deps has already resolved
# packages.yml into transform/lakehouse/dbt_packages (dbt_date, dbt_expectations,
# dbt_utils, elementary). Re-running it on every `dagster dev` restart trades a
# network dependency and startup latency for a convenience this project doesn't
# need: the manifest at transform/lakehouse/target/manifest.json is already current
# and rebuilt by the same `make build` step that runs the models themselves. After
# editing a dbt model, run `dbt parse --project-dir transform/lakehouse
# --profiles-dir transform/lakehouse` (or `make build`) and restart `dagster dev`
# to pick up the change.
