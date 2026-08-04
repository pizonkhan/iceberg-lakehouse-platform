"""Warehouse connection details and source-entity layout for bronze ingestion.

Credentials are read from the repository's .env, the same file `make up` passes to
docker compose, rather than duplicated into a second, committed secrets file. See
.notes/decisions.md.
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"
SOURCE_ROOT = REPO_ROOT / "generation" / "output"

# Iceberg namespace (Nessie namespace / Trino schema) all bronze tables are created in.
BRONZE_NAMESPACE = "bronze"

# Nessie's catalog identifier for the warehouse, fixed in infra/docker-compose.yml as
# `nessie.catalog.default-warehouse: warehouse`. This is a literal catalog-side name,
# not derived from MINIO_BUCKET, even though the .env default happens to use the same
# string for both today.
NESSIE_WAREHOUSE_NAME = "warehouse"

INGESTION_DIR = Path(__file__).resolve().parent
_DLT_PROJECT_DIR = INGESTION_DIR / ".dlt"
_DLT_SECRETS_PATH = _DLT_PROJECT_DIR / "secrets.toml"

# dlt resolves its `.dlt/` settings folder relative to the current working directory by
# default, which would be the repo root for every make/uv invocation in this project,
# not this package's own directory. DLT_PROJECT_DIR overrides that. Must be set before
# dlt resolves any config (see install_dlt_project_dir(), called at pipeline start).


def install_dlt_project_dir() -> None:
    """Point dlt's config/secrets resolution at ingestion/.dlt regardless of cwd."""
    os.environ.setdefault("DLT_PROJECT_DIR", str(INGESTION_DIR))


def _load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


class WarehouseConfig(BaseModel):
    """Resolved connection details for MinIO (S3) and Nessie (Iceberg REST catalog)."""

    model_config = ConfigDict(frozen=True)

    minio_user: str
    minio_password: str
    minio_bucket: str
    minio_api_port: str
    nessie_port: str

    @property
    def s3_endpoint(self) -> str:
        return f"http://127.0.0.1:{self.minio_api_port}"

    @property
    def nessie_rest_uri(self) -> str:
        return f"http://127.0.0.1:{self.nessie_port}/iceberg"


def load_warehouse_config() -> WarehouseConfig:
    """Load MinIO/Nessie connection details from the environment or repo .env.

    Real environment variables win over .env, matching how docker compose itself
    resolves --env-file values (a shell export overrides the file).
    """
    env = {**_load_env_file(ENV_FILE), **os.environ}
    required = ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_BUCKET")
    missing = [key for key in required if key not in env]
    if missing:
        raise RuntimeError(
            f"Missing warehouse credentials: {', '.join(missing)}. Copy .env.example to "
            ".env and fill it in before running ingestion (the same file `make up` uses)."
        )
    return WarehouseConfig(
        minio_user=env["MINIO_ROOT_USER"],
        minio_password=env["MINIO_ROOT_PASSWORD"],
        minio_bucket=env["MINIO_BUCKET"],
        minio_api_port=env.get("MINIO_API_PORT", "9000"),
        nessie_port=env.get("NESSIE_PORT", "19120"),
    )


def render_dlt_secrets(warehouse: WarehouseConfig) -> None:
    """(Re)write ingestion/.dlt/secrets.toml from the resolved warehouse config.

    dlt's Iceberg REST catalog config is a nested dict (see .notes/decisions.md for why
    it must come from a TOML file rather than env vars), so it needs a real file on
    disk. Generating it at the start of every run, from .env, keeps .env as the single
    source of truth for local credentials instead of forking them into a second file
    that could drift out of sync. This file is gitignored.
    """
    _DLT_PROJECT_DIR.mkdir(parents=True, exist_ok=True)
    content = f"""\
# generated at ingestion run start from the repository .env, do not edit by hand
[iceberg_catalog]
iceberg_catalog_name = "nessie"
iceberg_catalog_type = "rest"

[iceberg_catalog.iceberg_catalog_config]
uri = "{warehouse.nessie_rest_uri}"
type = "rest"
warehouse = "{NESSIE_WAREHOUSE_NAME}"
"py-io-impl" = "pyiceberg.io.pyarrow.PyArrowFileIO"
"s3.endpoint" = "{warehouse.s3_endpoint}"
"s3.access-key-id" = "{warehouse.minio_user}"
"s3.secret-access-key" = "{warehouse.minio_password}"
"s3.region" = "us-east-1"
"s3.path-style-access" = "true"
"""
    _DLT_SECRETS_PATH.write_text(content)


class EntitySource(BaseModel):
    """One generation/output/ subdirectory mapped to one bronze Iceberg table."""

    model_config = ConfigDict(frozen=True)

    table_name: str
    directory: str


# Table naming: `bronze_<entity>` inside the `bronze` namespace (e.g. the Trino
# identifier `iceberg.bronze.bronze_subscriber_events`). The prefix is redundant with
# the namespace but matches the table names fixed in the ingestion work package
# directly, see .notes/decisions.md.
ENTITIES: tuple[EntitySource, ...] = (
    EntitySource(table_name="bronze_plans", directory="plans"),
    EntitySource(table_name="bronze_devices", directory="devices"),
    EntitySource(table_name="bronze_title_genres", directory="title_genres"),
    EntitySource(table_name="bronze_title_events", directory="title_events"),
    EntitySource(table_name="bronze_subscriber_events", directory="subscriber_events"),
    EntitySource(table_name="bronze_billing_ledger", directory="billing_ledger"),
    EntitySource(table_name="bronze_watchlist_adds", directory="watchlist_adds"),
    EntitySource(table_name="bronze_signup_funnel", directory="signup_funnel"),
    # Largest by a wide margin (~120M rows) and the one with mid-stream schema drift
    # (playback_quality appears partway through), kept last so smaller tables finish
    # first and a partial run still leaves useful bronze data in place.
    EntitySource(table_name="bronze_playback_sessions", directory="playback_sessions"),
)
