"""Bronze ingestion entrypoint.

Reads every parquet file under generation/output/ and loads it, append-only, into one
Iceberg table per source entity in the `bronze` namespace of the local Nessie/MinIO
warehouse. Safe to run repeatedly and safe to run from a fresh clone; see
.notes/decisions.md for how idempotency and replayability actually work.

Usage:
    uv run python -m ingestion.pipeline                  # all entities
    uv run python -m ingestion.pipeline bronze_plans      # one entity
    uv run python -m ingestion.pipeline bronze_plans,bronze_devices
"""

from __future__ import annotations

import sys
import uuid
from datetime import UTC, datetime

import dlt
import structlog

from ingestion.network import install_docker_host_aliases
from ingestion.settings import (
    BRONZE_NAMESPACE,
    ENTITIES,
    SOURCE_ROOT,
    install_dlt_project_dir,
    load_warehouse_config,
    render_dlt_secrets,
)
from ingestion.sources import make_entity_resource

logger = structlog.get_logger()


def _new_batch_id(run_started_at: datetime) -> str:
    """One identifier shared by every row loaded in this run, across all tables.

    Not dlt's own `_dlt_load_id`: that column already exists on every row for free,
    but tying the bronze contract to a dlt-internal column name would leak a tooling
    detail into an interface silver depends on. Generating our own keeps that contract
    stable even if the ingestion tool changes later.
    """
    return f"{run_started_at:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def run(entity_names: set[str] | None = None) -> None:
    """Run bronze ingestion. If entity_names is given, load only those tables."""
    if not SOURCE_ROOT.is_dir():
        raise RuntimeError(
            f"{SOURCE_ROOT} does not exist. Bronze ingestion reads generation/output/ as "
            "produced by the synthetic data generator; run that first."
        )

    install_dlt_project_dir()
    install_docker_host_aliases()
    warehouse = load_warehouse_config()
    render_dlt_secrets(warehouse)

    run_started_at = datetime.now(UTC)
    batch_id = _new_batch_id(run_started_at)
    logger.info("starting bronze ingestion run", batch_id=batch_id)

    entities = [e for e in ENTITIES if entity_names is None or e.table_name in entity_names]
    if not entities:
        raise ValueError(f"no known entity matches {entity_names!r}")

    destination = dlt.destinations.filesystem(
        bucket_url=f"s3://{warehouse.minio_bucket}",
        credentials={
            "aws_access_key_id": warehouse.minio_user,
            "aws_secret_access_key": warehouse.minio_password,
            "endpoint_url": warehouse.s3_endpoint,
        },
    )
    pipeline = dlt.pipeline(
        pipeline_name="bronze_ingestion",
        destination=destination,
        dataset_name=BRONZE_NAMESPACE,
    )

    resources = [make_entity_resource(e, batch_id, run_started_at) for e in entities]
    load_info = pipeline.run(resources)

    logger.info("bronze ingestion run complete", batch_id=batch_id)
    print(load_info)


def main() -> None:
    entity_arg = sys.argv[1] if len(sys.argv) > 1 else None
    entity_names = set(entity_arg.split(",")) if entity_arg else None
    run(entity_names)


if __name__ == "__main__":
    main()
