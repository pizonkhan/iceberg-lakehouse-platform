"""Shared fixtures for the live-Trino integration suite.

Every test under tests/integration/ queries the real running stack (Trino
on TRINO_HOST:TRINO_PORT, catalog "iceberg") rather than mocking anything:
the point of this suite is to prove the gold-layer contracts in
.notes/modeling.md hold against actually-built data, per the work package
brief that commissioned it. Nothing here is meaningful, or even runnable,
without the docker compose stack up (`make up`).

Never mutates dev_dimensions, dev_facts, dev_silver, or bronze, even
temporarily: fail-then-fix proofs that need to write bad data operate
exclusively under iceberg.test_scratch, created and torn down by the
fixtures below.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator

import pytest
import trino.dbapi
from trino.dbapi import Connection

CATALOG = "iceberg"
SCRATCH_SCHEMA = "test_scratch"
SCRATCH_LOCATION = f"s3://warehouse/{SCRATCH_SCHEMA}/"


def _connect() -> Connection:
    # every query in this suite uses fully-qualified catalog.schema.table
    # names, so the connection's own default schema is never relied on;
    # it just needs to be a schema that exists.
    #
    # trino ships py.typed, but dbapi.connect() itself is defined as
    # `def connect(*args, **kwargs)` with no annotations, a real upstream
    # gap (same issue and same fix as ops/wap.py's _trino_execute).
    return trino.dbapi.connect(  # type: ignore[no-untyped-call,no-any-return]
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="pytest_integration",
        catalog=CATALOG,
        schema="dev_facts",
    )


@pytest.fixture(scope="session")
def trino_conn() -> Iterator[Connection]:
    """One connection shared for the whole session. Trino has no
    cross-query transaction state to isolate between tests here (every
    test either reads committed gold data or creates/drops its own
    uniquely-named scratch table), so reusing one connection avoids
    re-handshaking per test."""
    conn = _connect()
    yield conn
    conn.close()  # type: ignore[no-untyped-call]


@pytest.fixture(scope="session")
def scratch_schema(trino_conn: Connection) -> str:
    """Creates iceberg.test_scratch once per session for the fail-then-fix
    proofs (grain violation and SCD interval integrity tests). The schema
    itself is deliberately left in place at session end rather than
    dropped: every test that creates a table in it is responsible for
    dropping that table (see scratch_table below), so a crashed run
    leaves at most a stray table, not a torn-down schema a concurrent or
    next run depends on.
    """
    cur = trino_conn.cursor()
    cur.execute(
        f"create schema if not exists {CATALOG}.{SCRATCH_SCHEMA} "
        f"with (location = '{SCRATCH_LOCATION}')"
    )
    cur.fetchall()
    return SCRATCH_SCHEMA


ScratchTableFactory = Callable[[str, str], str]


@pytest.fixture
def scratch_table(trino_conn: Connection, scratch_schema: str) -> Iterator[ScratchTableFactory]:
    """Yields a factory `make(name, select_sql) -> fully_qualified_name`
    that CTAS-creates a table under iceberg.test_scratch and guarantees it
    is dropped after the test, pass or fail, so a failed fail-then-fix run
    never leaves debris behind for the next one to collide with.
    Function-scoped (not session-scoped) so every test gets its own clean
    teardown regardless of test order.
    """
    created: list[str] = []

    def make(name: str, select_sql: str) -> str:
        fq = f"{CATALOG}.{scratch_schema}.{name}"
        cur = trino_conn.cursor()
        cur.execute(f"drop table if exists {fq}")
        cur.fetchall()
        cur.execute(f"create table {fq} as {select_sql}")
        cur.fetchall()
        created.append(fq)
        return fq

    yield make

    cur = trino_conn.cursor()
    for fq in created:
        cur.execute(f"drop table if exists {fq}")
        cur.fetchall()
