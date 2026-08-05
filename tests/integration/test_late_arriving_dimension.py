"""Late-arriving dimension self-heal.

modeling.md's "Late-arriving subscriber handling" requires that a fact
referencing a subscriber_id dim_subscriber does not yet know about still
resolves to a real, stable subscriber_sk via a synthesized inferred row
(subscriber_sk = md5(subscriber_id, 1900-01-01), is_inferred = true),
never falls back to the unknown member, and is never orphaned.

Fixture: generation/output/_pathology_manifest/late_arriving_subscribers.csv,
200 subscriber_ids (verified by reading the file: a single subscriber_id
column) the generator deliberately made appear in a fact-side event stream
without a corresponding subscriber_events record at the time those events
were generated.

dim_subscriber is built full-refresh (see dim_subscriber.sql's header
comment and .notes/decisions.md), and this project's current bronze
history is a single load, so by the time this test runs every one of
these 200 subscribers has, in practice, already been built through the
normal versioned path rather than left as an is_inferred=true row
(confirmed directly: is_inferred=true count is 0 in the real table today).
modeling.md's own inferred-row spec anticipates exactly this: "the moment
real profile events exist for a subscriber_id, that id no longer appears
in late_arriving_ids at all." Per the work package brief, these tests
therefore check both possible states rather than assuming one: an
is_inferred=true row is validated against its documented shape if one
exists, and every manifest subscriber_id is checked for having been
resolved and not orphaned regardless of which state it landed in.
"""

from __future__ import annotations

import pytest
from trino.dbapi import Connection

from tests.integration._helpers import fetch_all, read_manifest, scalar, sql_in_list

LATE_ARRIVAL_SENTINEL_EFFECTIVE_FROM = "1900-01-01 00:00:00.000000"

# (fact_table, silver_table, fact_id_column, silver_id_column, event_time_column):
# the three fact-side silver streams dim_subscriber.sql itself unions to detect
# late arrivals (its fact_subscriber_ids cte), so these are exactly the facts
# that can legitimately reference a not-yet-seen subscriber_id.
LATE_ARRIVAL_TOUCHING_FACTS = [
    (
        "fct_playback_events",
        "silver_playback_sessions",
        "playback_session_id",
        "playback_session_id",
    ),
    (
        "fct_billing_transactions",
        "silver_billing_ledger",
        "billing_transaction_id",
        "billing_transaction_id",
    ),
    ("fct_watchlist_adds", "silver_watchlist_adds", "watchlist_event_id", "watchlist_event_id"),
]


@pytest.fixture(scope="module")
def late_arriving_ids() -> list[str]:
    rows = read_manifest("late_arriving_subscribers.csv")
    assert rows and "subscriber_id" in rows[0], (
        "late_arriving_subscribers.csv is empty or missing subscriber_id; fixture shape changed"
    )
    return [r["subscriber_id"] for r in rows]


def test_late_arriving_subscribers_are_not_orphaned(
    trino_conn: Connection, late_arriving_ids: list[str]
) -> None:
    """Every manifest subscriber_id must exist in dim_subscriber, whether it
    landed there as a still-inferred row or was fully backfilled through
    the normal versioned path by build time. A count short of the manifest
    size would mean a subscriber_id that facts reference has no dimension
    row at all, the exact orphan condition the inferred-row mechanism
    exists to prevent."""
    present = scalar(
        trino_conn,
        f"""
        select count(distinct subscriber_id)
        from iceberg.dev_dimensions.dim_subscriber
        where subscriber_id in ({sql_in_list(late_arriving_ids)})
        """,
    )
    assert present == len(late_arriving_ids), (
        f"expected all {len(late_arriving_ids)} late-arriving subscriber_ids present in "
        f"dim_subscriber, found {present}"
    )


def test_any_remaining_inferred_rows_match_the_documented_shape(
    trino_conn: Connection, late_arriving_ids: list[str]
) -> None:
    """For any manifest subscriber_id still carrying an is_inferred=true row
    (none, in the current fully-backfilled dataset, but this must not be
    assumed), the row must match modeling.md's exact inferred-row spec:
    effective_from pinned at the 1900-01-01 sentinel, exactly one such row
    per subscriber_id (a second inferred row would mean the backfill
    update-in-place path failed and inserted a duplicate instead)."""
    sentinel_ts = f"timestamp '{LATE_ARRIVAL_SENTINEL_EFFECTIVE_FROM}'"
    rows = fetch_all(
        trino_conn,
        f"""
        select subscriber_id, count(*) as inferred_row_count,
               count(*) filter (where effective_from = {sentinel_ts}) as sentinel_row_count
        from iceberg.dev_dimensions.dim_subscriber
        where is_inferred and subscriber_id in ({sql_in_list(late_arriving_ids)})
        group by subscriber_id
        """,
    )
    if not rows:
        pytest.skip(
            "no is_inferred=true rows remain for any manifest subscriber_id "
            "(fully backfilled in the current dataset); nothing to validate the shape of"
        )
    bad_rows = [r for r in rows if r[1] != 1 or r[2] != 1]
    assert not bad_rows, f"inferred row(s) not matching the documented shape: {bad_rows}"


@pytest.mark.parametrize(
    "fact_table, silver_table, fact_id_col, silver_id_col",
    LATE_ARRIVAL_TOUCHING_FACTS,
    ids=[f[0] for f in LATE_ARRIVAL_TOUCHING_FACTS],
)
def test_late_arriving_subscriber_facts_do_not_fall_back_to_unknown_member(
    trino_conn: Connection,
    late_arriving_ids: list[str],
    fact_table: str,
    silver_table: str,
    fact_id_col: str,
    silver_id_col: str,
) -> None:
    """The gold fact tables carry no subscriber_id column (only
    subscriber_sk), so a row that fell back to the unknown member cannot be
    traced back to its original subscriber_id from gold alone: this joins
    forward from the silver layer, which still has subscriber_id, through
    each fact's own degenerate-dimension id, to gold's resolved
    subscriber_sk, and asserts that key is never the unknown member's key
    for any silver row originally attributed to a late-arriving
    subscriber_id.
    """
    unknown_sk = scalar(
        trino_conn,
        "select subscriber_sk from iceberg.dev_dimensions.dim_subscriber "
        "where subscriber_id = '-1'",
    )
    assert unknown_sk, (
        "could not find the unknown member row (subscriber_id = '-1') in dim_subscriber"
    )

    fallbacks = scalar(
        trino_conn,
        f"""
        select count(*)
        from iceberg.dev_silver.{silver_table} s
        join iceberg.dev_facts.{fact_table} f on f.{fact_id_col} = s.{silver_id_col}
        where s.subscriber_id in ({sql_in_list(late_arriving_ids)})
        and f.subscriber_sk = '{unknown_sk}'
        """,
    )
    assert fallbacks == 0, (
        f"{fallbacks} row(s) in {fact_table} for a late-arriving subscriber_id resolved to "
        "the unknown member instead of a real or inferred subscriber_sk"
    )
