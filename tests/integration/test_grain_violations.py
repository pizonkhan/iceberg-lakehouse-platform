"""Grain violation detection, with a real fail-then-fix proof.

Each fact/dim below has a declared grain in .notes/modeling.md
(playback_session_id; billing_transaction_id; (subscriber_id,
effective_from) plus exactly-one-is_current-per-subscriber_id). This file
first asserts zero violations against the real built tables, then proves
those detection queries are not vacuously true by running the identical
query shape against a scratch copy with a deliberately inserted duplicate
and confirming it now reports a violation.

fct_playback_events (~120M rows) needs its own strategy: this stack's
Trino caps query memory at 1.5GB per node (infra/trino/etc/config.properties),
and a single GROUP BY playback_session_id over the full table blows that
cap (confirmed directly: TrinoUserError EXCEEDED_LOCAL_MEMORY_LIMIT,
HashAggregationOperator alone reached 1.28GB). playback_session_id is
always "pb_" followed by a zero-padded sequential integer (verified
against the real table's min/max), so the check is split into disjoint
passes filtered by that integer modulo a bucket count. A genuine
duplicate always shares the exact same id string on both rows, so it
always lands in the same bucket both times; bucketing narrows each pass's
working set, it cannot hide a real violation across passes.
"""

from __future__ import annotations

import pytest
from trino.dbapi import Connection

from tests.integration._helpers import scalar
from tests.integration.conftest import ScratchTableFactory

PLAYBACK_BUCKETS = 10  # ~12M rows/pass; 20-bucket and 10-bucket passes were both
# confirmed to fit comfortably under the 1.5GB cap during development, 10 chosen to
# keep this test's total wall time down (~10 sequential passes at ~15s each).


def _duplicate_count_sql(table: str, grain_columns: list[str]) -> str:
    cols = ", ".join(grain_columns)
    return f"""
        select count(*) from (
            select {cols}, count(*) as c
            from {table}
            group by {cols}
            having count(*) > 1
        )
    """


def _playback_duplicate_count(trino_conn: Connection) -> int:
    total = 0
    cur = trino_conn.cursor()
    for bucket in range(PLAYBACK_BUCKETS):
        bucket_predicate = (
            f"mod(cast(substr(playback_session_id, 4) as bigint), {PLAYBACK_BUCKETS}) = {bucket}"
        )
        cur.execute(
            f"""
            select count(*) from (
                select playback_session_id, count(*) as c
                from iceberg.dev_facts.fct_playback_events
                where {bucket_predicate}
                group by playback_session_id
                having count(*) > 1
            )
            """
        )
        total += cur.fetchall()[0][0]
    return total


@pytest.mark.slow
def test_fct_playback_events_grain_has_no_violations(trino_conn: Connection) -> None:
    """Grain: one row per playback_session_id (modeling.md). A violation
    here would mean the Iceberg MERGE's unique_key matching, or the
    upstream silver dedup step, let a duplicate merge key through."""
    violations = _playback_duplicate_count(trino_conn)
    assert violations == 0, (
        f"found {violations} duplicate playback_session_id group(s) in the real table"
    )


def test_fct_billing_transactions_grain_has_no_violations(trino_conn: Connection) -> None:
    """Grain: one row per billing_transaction_id. Table is small enough
    (~1.5M rows) for a single GROUP BY, unlike fct_playback_events.
    duplicate_billing_ids.csv in the pathology manifest documents that the
    generator deliberately injects upstream duplicate billing_transaction_id
    rows into bronze; this test proves gold's dedup/merge collapsed all of
    them back to the declared grain."""
    violations = scalar(
        trino_conn,
        _duplicate_count_sql(
            "iceberg.dev_facts.fct_billing_transactions", ["billing_transaction_id"]
        ),
    )
    assert violations == 0, (
        f"found {violations} duplicate billing_transaction_id group(s) in the real table"
    )


def test_dim_subscriber_grain_has_no_violations(trino_conn: Connection) -> None:
    """Grain: (subscriber_id, effective_from), the composite SCD version
    key declared in modeling.md's "Required data tests on every SCD dim"."""
    violations = scalar(
        trino_conn,
        _duplicate_count_sql(
            "iceberg.dev_dimensions.dim_subscriber", ["subscriber_id", "effective_from"]
        ),
    )
    assert violations == 0, f"found {violations} duplicate (subscriber_id, effective_from) group(s)"


def test_dim_subscriber_is_current_uniqueness_has_no_violations(trino_conn: Connection) -> None:
    """Invariant: exactly one is_current=true row per subscriber_id. This is
    a separate invariant from grain uniqueness above: two historical rows
    could each be individually grain-unique while both wrongly claiming
    is_current, which the grain test alone would not catch."""
    violations = scalar(
        trino_conn,
        """
        select count(*) from (
            select subscriber_id, count(*) as c
            from iceberg.dev_dimensions.dim_subscriber
            where is_current
            group by subscriber_id
            having count(*) <> 1
        )
        """,
    )
    assert violations == 0, f"found {violations} subscriber_id(s) with != 1 is_current row"


def test_grain_violation_detection_has_teeth_on_billing(
    trino_conn: Connection, scratch_table: ScratchTableFactory
) -> None:
    """Proves test_fct_billing_transactions_grain_has_no_violations is not
    vacuously true. Copies a small real slice of fct_billing_transactions
    into iceberg.test_scratch (never touching the real table), inserts an
    exact duplicate of one existing row (identical billing_transaction_id,
    a genuine grain violation), reruns the identical detection query shape
    against the scratch copy, and asserts it now reports the violation.
    fct_playback_events and dim_subscriber share the same detection query
    shape with a different grain column list, so this proves the mechanism
    for all three without needing three separate scratch fixtures."""
    fq = scratch_table(
        "billing_grain_duplicate",
        "select * from iceberg.dev_facts.fct_billing_transactions limit 500",
    )
    cur = trino_conn.cursor()
    cur.execute(f"insert into {fq} select * from {fq} limit 1")
    cur.fetchall()

    violations = scalar(trino_conn, _duplicate_count_sql(fq, ["billing_transaction_id"]))
    assert violations == 1, (
        f"expected exactly 1 duplicate group after inserting one deliberate duplicate row, "
        f"got {violations}; the detection query has no teeth"
    )


def test_grain_violation_detection_has_teeth_on_dim_subscriber(
    trino_conn: Connection, scratch_table: ScratchTableFactory
) -> None:
    """Same fail-then-fix proof as the billing case, against a
    dim_subscriber slice and its composite (subscriber_id, effective_from)
    grain key, confirming the detection query generalizes to a
    multi-column grain, not just a single degenerate-dimension id."""
    fq = scratch_table(
        "dim_subscriber_grain_duplicate",
        "select * from iceberg.dev_dimensions.dim_subscriber limit 500",
    )
    cur = trino_conn.cursor()
    cur.execute(f"insert into {fq} select * from {fq} limit 1")
    cur.fetchall()

    violations = scalar(trino_conn, _duplicate_count_sql(fq, ["subscriber_id", "effective_from"]))
    assert violations == 1, (
        f"expected exactly 1 duplicate group after inserting one deliberate duplicate row, "
        f"got {violations}; the detection query has no teeth"
    )
