"""dlt resource definitions for the bronze layer.

Each resource reads every parquet file under one generation/output/<entity>/ directory
and attaches the four bronze metadata columns (_source_file, _ingested_at, _batch_id,
_payload_hash) before yielding pyarrow record batches to dlt. Reading goes through
DuckDB (already a project dependency) because it gives a vectorized md5() for
_payload_hash and a streaming Arrow record-batch reader in one pass, rather than a
separate row-hashing step.

Source columns are read per file, not fixed per entity, so a file with fewer columns
than a later file in the same directory (the playback_sessions mid-stream schema drift)
is read and hashed using only the columns it actually has. dlt's Iceberg writer then
evolves the bronze table by union-of-columns across loads: rows from files written
before a column existed simply read back as NULL for it, nothing is dropped or
rewritten. See .notes/decisions.md.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import dlt
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from ingestion.hashing import payload_hash_expression
from ingestion.settings import SOURCE_ROOT, EntitySource

logger = structlog.get_logger()

# Rows per yielded Arrow record batch. Large enough to keep DuckDB and dlt's parquet
# writer efficient, small enough that one batch is a bounded, predictable chunk of
# memory regardless of how large the source file is (the playback_sessions files run
# to several million rows each).
BATCH_SIZE = 500_000


def _read_file(file_path: Path, batch_id: str, ingested_at: datetime) -> Iterator[pa.RecordBatch]:
    """Read one parquet file, attach bronze metadata columns, yield Arrow batches."""
    source_columns = pq.read_schema(file_path).names
    hash_expression = payload_hash_expression(source_columns)
    relative_path = str(file_path.relative_to(SOURCE_ROOT))

    con = duckdb.connect()
    try:
        # Pin the session timezone so _ingested_at lands as a UTC instant regardless
        # of the host's local timezone. Source timestamp columns are untouched by this,
        # DuckDB only applies session timezone to values it receives without one.
        con.execute("SET TimeZone='UTC'")
        query = f"""
            SELECT
                *,
                ? AS _source_file,
                ? AS _ingested_at,
                ? AS _batch_id,
                {hash_expression} AS _payload_hash
            FROM read_parquet(?)
        """
        result = con.execute(query, [relative_path, ingested_at, batch_id, str(file_path)])
        reader = result.fetch_record_batch(BATCH_SIZE)
        yield from reader
    finally:
        con.close()


def make_entity_resource(
    entity: EntitySource, batch_id: str, ingested_at: datetime
) -> dlt.sources.DltResource:
    """Build the dlt resource that loads one bronze table.

    Idempotency and replayability both come from tracking, in dlt's own pipeline
    state, which source files this resource has already loaded. dlt persists that
    state into the same load package as the data it describes, so the two can never
    drift out of sync: a file is marked processed if and only if its rows actually
    committed. Re-running against unchanged files is a no-op; new files dropped into
    generation/output/ get appended on the next run; a fresh clone pointed at an empty
    warehouse has no state and reprocesses everything from scratch, reaching the same
    bronze state byte for byte. Bronze does not deduplicate or clean rows within a
    file, that stays silver's job; the unit of replay-safety here is the source file,
    not the row. See .notes/decisions.md.
    """

    @dlt.resource(
        name=entity.table_name,
        table_format="iceberg",
        write_disposition="append",
    )
    def _resource() -> Iterator[pa.RecordBatch]:
        state = dlt.current.resource_state()
        processed: list[str] = state.setdefault("processed_files", [])
        processed_set = set(processed)

        files = sorted((SOURCE_ROOT / entity.directory).glob("*.parquet"))
        if not files:
            logger.warning("no source files found", directory=entity.directory)
            return

        for file_path in files:
            relative_path = str(file_path.relative_to(SOURCE_ROOT))
            if relative_path in processed_set:
                logger.debug("skipping already-loaded file", file=relative_path)
                continue
            logger.info("ingesting file", table=entity.table_name, file=relative_path)
            yield from _read_file(file_path, batch_id, ingested_at)
            processed.append(relative_path)

    return _resource
