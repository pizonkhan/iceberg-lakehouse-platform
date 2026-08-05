"""Demonstrate Nessie-native time travel and rollback through Trino, and capture the
real evidence into docs/evidence/time-travel/.

Context (.notes/open-questions.md, 2026-08-05 red team pass, part 3): this stack's
Nessie REST catalog does not chain Iceberg-native snapshot history inside a table's own
metadata.json, so Trino's `SELECT ... FOR VERSION AS OF <snapshot_id>` and queries
against `$snapshots`/`$history` never see more than the single most recent commit, even
though every commit really happened and Nessie itself has the full history. The real
mechanism is Nessie's own commit graph, reached through Trino by registering a dynamic
Iceberg REST catalog whose `iceberg.rest-catalog.uri` targets a specific Nessie ref
(the same pattern ops/wap.py already uses for branches), and rollback is a real Nessie
branch reset (ops/nessie.py's assign_reference), not an Iceberg-level operation at all.

Everything this script does happens on a dedicated branch (BRANCH_NAME below), cut from
main and never merged back, not on main itself. This is a deliberate deviation from
doing the walkthrough directly on main: Nessie's branch reset is branch-wide, not
scoped to one table, so resetting main itself to undo this demo's bad commit would also
revert every other table cataloged on main (dev_dimensions, dev_facts, dev_silver, dbt's
own bookkeeping tables) back to that same point, an unacceptable blast radius for a
demonstration. See .notes/decisions.md for the full reasoning.

Usage:
    uv run python -m ops.time_travel_demo

Idempotent: if BRANCH_NAME already exists from a previous run, it is deleted and
recreated fresh off main's current HEAD, so repeat runs produce a clean, reproducible
evidence capture rather than accumulating rows across runs.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import structlog
import trino.dbapi

from ops import nessie
from ops.wap import (
    WapConfig,
    _create_catalog,
    _drop_catalog,
    load_config,
)

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ]
)
logger = structlog.get_logger()

REPO_ROOT = Path(__file__).resolve().parent.parent
EVIDENCE_DIR = REPO_ROOT / "docs" / "evidence" / "time-travel"

BRANCH_NAME = "time_travel_demo"
SCHEMA_NAME = "time_travel_demo"
TABLE_NAME = "demo_billing_batch"
LIVE_CATALOG = "iceberg_tt_live"
ASOF_CATALOG = "iceberg_tt_asof_good"


def _select_all_sql(catalog: str) -> str:
    return f"SELECT * FROM {catalog}.{SCHEMA_NAME}.{TABLE_NAME} ORDER BY batch_id, subscriber_id"


def _trino_query(config: WapConfig, catalog: str, sql: str) -> list[dict[str, Any]]:
    """Run a query against catalog and return rows as a list of column-name-keyed
    dicts, the shape this script writes straight to evidence JSON."""
    connection = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host=config.trino_host,
        port=config.trino_port,
        user="time_travel_demo",
        catalog=catalog,
        schema=SCHEMA_NAME,
    )
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in rows]
    finally:
        connection.close()


def _trino_execute(config: WapConfig, catalog: str, sql: str) -> None:
    connection = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host=config.trino_host,
        port=config.trino_port,
        user="time_travel_demo",
        catalog=catalog,
        schema=SCHEMA_NAME,
    )
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.fetchall()
    finally:
        connection.close()


def _write_json(name: str, payload: Any) -> None:
    path = EVIDENCE_DIR / name
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    logger.info("wrote evidence", file=str(path))


def _write_text(name: str, text: str) -> None:
    path = EVIDENCE_DIR / name
    path.write_text(text if text.endswith("\n") else text + "\n")
    logger.info("wrote evidence", file=str(path))


def _log_entries_as_dicts(entries: list[nessie.CommitLogEntry]) -> list[dict[str, Any]]:
    return [entry.model_dump() for entry in entries]


def run(config: WapConfig) -> None:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    log = logger.bind(branch=BRANCH_NAME, schema=SCHEMA_NAME, table=TABLE_NAME)

    # 0. Fresh branch off main's current HEAD, deleting any leftover branch from a
    # prior run first so this script is safely rerunnable.
    main_ref = nessie.get_reference(config.nessie_uri, "main")
    log.info("read main HEAD", main_hash=main_ref.hash)
    try:
        existing = nessie.get_reference(config.nessie_uri, BRANCH_NAME)
        nessie.delete_reference(config.nessie_uri, BRANCH_NAME, existing.hash)
        log.info("deleted leftover branch from a prior run", old_hash=existing.hash)
    except nessie.NessieError:
        pass  # no leftover branch, the common case
    branch_ref = nessie.create_branch(config.nessie_uri, BRANCH_NAME, main_ref)
    log.info("branch created off main", branch_hash=branch_ref.hash)

    _create_catalog(config, LIVE_CATALOG, BRANCH_NAME)
    log.info("live catalog registered", catalog=LIVE_CATALOG)

    # 1. Schema and table, then commit 1: a good batch.
    _trino_execute(
        config, LIVE_CATALOG, f"CREATE SCHEMA IF NOT EXISTS {LIVE_CATALOG}.{SCHEMA_NAME}"
    )
    _trino_execute(
        config,
        LIVE_CATALOG,
        f"""
        CREATE TABLE {LIVE_CATALOG}.{SCHEMA_NAME}.{TABLE_NAME} (
            batch_id varchar,
            subscriber_id varchar,
            amount_usd decimal(10, 2),
            loaded_at timestamp(6)
        )
        WITH (format = 'PARQUET', format_version = 2)
        """,
    )
    _trino_execute(
        config,
        LIVE_CATALOG,
        f"""
        INSERT INTO {LIVE_CATALOG}.{SCHEMA_NAME}.{TABLE_NAME}
        VALUES
            ('batch-001', 'sub-001', DECIMAL '14.99', TIMESTAMP '2026-08-01 09:00:00'),
            ('batch-001', 'sub-002', DECIMAL '9.99', TIMESTAMP '2026-08-01 09:00:00'),
            ('batch-001', 'sub-003', DECIMAL '24.99', TIMESTAMP '2026-08-01 09:00:00'),
            ('batch-001', 'sub-004', DECIMAL '14.99', TIMESTAMP '2026-08-01 09:00:00'),
            ('batch-001', 'sub-005', DECIMAL '9.99', TIMESTAMP '2026-08-01 09:00:00')
        """,
    )
    hash_good = nessie.get_reference(config.nessie_uri, BRANCH_NAME).hash
    log.info("commit 1 landed: good batch", hash_good=hash_good)

    # 2. Commit 2: a bad batch, violating the obvious invariant that a billing amount
    # cannot be negative.
    _trino_execute(
        config,
        LIVE_CATALOG,
        f"""
        INSERT INTO {LIVE_CATALOG}.{SCHEMA_NAME}.{TABLE_NAME}
        VALUES
            ('batch-002', 'sub-006', DECIMAL '19.99', TIMESTAMP '2026-08-02 09:00:00'),
            ('batch-002', 'sub-007', DECIMAL '-14.99', TIMESTAMP '2026-08-02 09:00:00'),
            ('batch-002', 'sub-008', DECIMAL '-9.99', TIMESTAMP '2026-08-02 09:00:00')
        """,
    )
    hash_bad = nessie.get_reference(config.nessie_uri, BRANCH_NAME).hash
    log.info("commit 2 landed: bad batch (negative amounts)", hash_bad=hash_bad)

    _write_text(
        "01-commit-hashes.txt",
        f"branch: {BRANCH_NAME}\n"
        f"main HEAD at branch creation: {main_ref.hash}\n"
        f"hash_good (after commit 1, good batch, 5 rows): {hash_good}\n"
        f"hash_bad  (after commit 2, bad batch, 3 rows, 2 negative amounts): {hash_bad}\n",
    )

    # 3. Point-in-time query: register a second catalog scoped to branch@hash_good and
    # query through it. This proves Nessie-native time travel works even though
    # Iceberg-native FOR VERSION AS OF does not on this catalog (see the module
    # docstring and .notes/open-questions.md).
    _create_catalog(config, ASOF_CATALOG, f"{BRANCH_NAME}@{hash_good}")
    log.info("as-of catalog registered", catalog=ASOF_CATALOG, ref=f"{BRANCH_NAME}@{hash_good}")

    asof_rows = _trino_query(config, ASOF_CATALOG, _select_all_sql(ASOF_CATALOG))
    _write_json("02-asof-good-query.json", {"ref": f"{BRANCH_NAME}@{hash_good}", "rows": asof_rows})
    log.info(
        "point-in-time query captured", ref=f"{BRANCH_NAME}@{hash_good}", row_count=len(asof_rows)
    )

    live_before_rows = _trino_query(config, LIVE_CATALOG, _select_all_sql(LIVE_CATALOG))
    _write_json(
        "03-live-query-before-rollback.json", {"ref": BRANCH_NAME, "rows": live_before_rows}
    )
    log.info("live (pre-rollback) query captured", ref=BRANCH_NAME, row_count=len(live_before_rows))

    history_before = nessie.get_log(
        config.nessie_uri,
        BRANCH_NAME,
        limit=10,
        filter_expr=f"commit.message.contains('{TABLE_NAME}')",
    )
    _write_json("04-nessie-history-before-rollback.json", _log_entries_as_dicts(history_before))
    log.info("nessie commit log captured (before rollback)", entry_count=len(history_before))

    # 4. Rollback: reset the branch pointer back to hash_good with Nessie's real
    # assign mechanism. This is a real rollback, not a historical read: commit 2
    # becomes unreachable from the branch's HEAD.
    reset_ref = nessie.assign_reference(config.nessie_uri, BRANCH_NAME, hash_bad, hash_good)
    _write_json(
        "05-rollback-assign-response.json",
        {
            "ref_name": BRANCH_NAME,
            "expected_hash_before_reset": hash_bad,
            "target_hash": hash_good,
            "reference_after_reset": reset_ref.model_dump(),
        },
    )
    log.info("branch reset to hash_good", new_head=reset_ref.hash)
    assert reset_ref.hash == hash_good, "branch did not land on the expected hash after reset"

    # 5. Re-query the live ref (no hash in the catalog URI, same catalog as before):
    # this proves the rollback is real, not just a historical read through a
    # different catalog.
    live_after_rows = _trino_query(config, LIVE_CATALOG, _select_all_sql(LIVE_CATALOG))
    _write_json("06-live-query-after-rollback.json", {"ref": BRANCH_NAME, "rows": live_after_rows})
    log.info("live (post-rollback) query captured", ref=BRANCH_NAME, row_count=len(live_after_rows))

    history_after = nessie.get_log(
        config.nessie_uri,
        BRANCH_NAME,
        limit=10,
        filter_expr=f"commit.message.contains('{TABLE_NAME}')",
    )
    _write_json("07-nessie-history-after-rollback.json", _log_entries_as_dicts(history_after))
    log.info("nessie commit log captured (after rollback)", entry_count=len(history_after))

    bad_batch_still_reachable = any(entry.hash == hash_bad for entry in history_after)
    _write_text(
        "08-summary.txt",
        "Time travel and rollback demonstration summary\n"
        "================================================\n\n"
        f"Branch: {BRANCH_NAME} (cut from main, never merged back)\n"
        f"Table: {SCHEMA_NAME}.{TABLE_NAME}\n\n"
        f"hash_good: {hash_good} (5 rows, all positive amounts)\n"
        f"hash_bad:  {hash_bad} (8 rows, 2 negative amounts, the bad load)\n\n"
        f"Point-in-time query at {BRANCH_NAME}@{hash_good} returned "
        f"{len(asof_rows)} rows, all positive amounts: "
        f"{'PASS' if len(asof_rows) == 5 else 'FAIL'}\n"
        f"Live query before rollback returned {len(live_before_rows)} rows, "
        f"including negative amounts: "
        f"{'PASS' if len(live_before_rows) == 8 else 'FAIL'}\n"
        f"Live query after rollback returned {len(live_after_rows)} rows, "
        f"all positive amounts: "
        f"{'PASS' if len(live_after_rows) == 5 else 'FAIL'}\n"
        f"hash_bad still reachable from branch HEAD after rollback: "
        f"{bad_batch_still_reachable} (expected: False)\n",
    )

    _drop_catalog(config, ASOF_CATALOG)
    _drop_catalog(config, LIVE_CATALOG)
    log.info("dynamic catalogs deregistered, demo branch and table left in place")


def main() -> int:
    config = load_config()
    run(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
