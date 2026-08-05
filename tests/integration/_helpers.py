"""Shared query and manifest helpers for the live-Trino integration suite.

Kept separate from conftest.py deliberately: conftest.py holds fixtures pytest
auto-discovers, this module is plain importable code. Mixing the two makes it
harder to tell, at a glance, which functions pytest wires up for you and which
are just library code.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from trino.dbapi import Connection

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_DIR = REPO_ROOT / "generation" / "output" / "_pathology_manifest"


def read_manifest(name: str) -> list[dict[str, str]]:
    """Reads a pathology manifest CSV as a list of column-name-keyed dicts.

    These files are generated (generation/output/ is gitignored, rebuildable
    from generation/ + SEED) so this reads whatever the current checkout's
    generator run actually produced, never a hardcoded copy of one run's
    output.
    """
    path = MANIFEST_DIR / name
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def fetch_all(conn: Connection, sql: str) -> list[tuple[Any, ...]]:
    cur = conn.cursor()
    cur.execute(sql)
    return list(cur.fetchall())


def scalar(conn: Connection, sql: str) -> Any:
    """Runs a query expected to return exactly one row, one column, and
    returns that value. Used throughout for count(*)-shaped checks."""
    rows = fetch_all(conn, sql)
    assert len(rows) == 1 and len(rows[0]) == 1, (
        f"expected a single scalar row from query, got {rows!r}: {sql}"
    )
    return rows[0][0]


def sql_str(value: str) -> str:
    """Quotes a Python string as a Trino VARCHAR literal, escaping embedded
    single quotes. Every value passed through here in this suite originates
    from a pathology manifest CSV (ids like sub_000096, pb_000120000000),
    not from anything resembling untrusted input, so this is about correct
    literal syntax, not injection defense."""
    return "'" + value.replace("'", "''") + "'"


def sql_ts(value: str) -> str:
    """Formats a manifest CSV timestamp string as a Trino TIMESTAMP literal.

    Manifest timestamps show up in two shapes depending on which generator
    code path wrote them: plain 'YYYY-MM-DD HH:MM:SS[.ffffff]' (Python
    datetime.__str__) or ISO 'YYYY-MM-DDTHH:MM:SS[.ffffff]' (datetime64's
    isoformat, used for hard_delete_at). Trino's TIMESTAMP literal syntax
    wants the space form, so the 'T' is normalized away unconditionally;
    this is a no-op on inputs that never had one.
    """
    return "timestamp '" + value.replace("T", " ") + "'"


def sql_in_list(values: list[str]) -> str:
    return ", ".join(sql_str(v) for v in values)
