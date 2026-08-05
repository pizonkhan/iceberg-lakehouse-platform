"""Dagster orchestration layer for the iceberg-lakehouse-platform project.

Wraps the already-built transform/lakehouse dbt project (staging, intermediate/
silver, marts/dimensions, marts/facts) as software-defined assets via dagster-dbt,
and adds one upstream asset for bronze ingestion, the one layer dagster-dbt does not
natively cover. This package does not rebuild or alter any dbt model: it generates
assets from dbt's own manifest.json, which is the source of truth for lineage.
"""

from __future__ import annotations
