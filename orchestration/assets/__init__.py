"""Asset definitions for the orchestration layer.

dbt_assets.py: software-defined assets generated from transform/lakehouse's dbt
manifest, one per staging/silver/marts model (dagster-dbt).

bronze.py: the one layer dagster-dbt does not cover, the dlt-based bronze
ingestion step (ingestion/pipeline.py), wired as the upstream asset the
dbt-generated staging assets depend on.
"""

from __future__ import annotations
