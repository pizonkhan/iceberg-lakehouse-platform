"""Soft and hard delete handling.

generation/subscribers.py injects two distinct delete pathologies (its
own pathology 10, cross-checked by generation/sanity_check.py's raw-parquet
sanity pass): a soft-deleted subscriber's final change event sets status to
'cancelled' or 'deleted' (generation/subscribers.py's SOFT_DELETE_STATUSES)
but the subscriber keeps existing and keeps being referenceable; a
hard-deleted subscriber has a hard_delete_at cutoff after which the
generator emits no further events of any kind for them (see subscribers.py:
"upper_bound = hard_delete_at" gates every follow-on event and playback/
billing/watchlist generation for that subscriber).

Gold-layer translation of each, read from the actual model code rather than
assumed: dim_subscriber.sql's "status remapping" cte remaps both
'cancelled' and 'deleted' onto this project's own status domain value
'churned' before anything else (row_hash, versioning, churn_date_key) ever
sees status, so 'churned' (not 'cancelled' or 'deleted') is the correct
terminal status to assert for a soft-deleted subscriber in gold. There is
no equivalent gold-side remapping for hard deletes: the generator simply
never produces events past hard_delete_at, so the assertion is an absence
check on each fact table's own event-time column.
"""

from __future__ import annotations

import pytest
from trino.dbapi import Connection

from tests.integration._helpers import fetch_all, read_manifest, scalar, sql_in_list, sql_ts

SOFT_DELETE_TERMINAL_STATUS = "churned"

# (fact_table, event_time_column): every fact that carries a subscriber_sk
# and an event timestamp, per the bus matrix. fct_daily_subscription_snapshot
# and fct_signup_funnel are deliberately excluded: a snapshot fact continues
# to emit rows for a churned-but-not-hard-deleted subscriber by design (that
# is a different pathology, soft delete, not this check), and a signup
# funnel row is pinned once at registration, before any delete pathology
# could apply to it.
HARD_DELETE_TOUCHING_FACTS = [
    ("fct_playback_events", "session_started_at"),
    ("fct_billing_transactions", "transaction_posted_at"),
    ("fct_watchlist_adds", "added_at"),
]


@pytest.fixture(scope="module")
def soft_deleted_ids() -> list[str]:
    rows = read_manifest("soft_deleted_subscribers.csv")
    assert rows and "subscriber_id" in rows[0], (
        "soft_deleted_subscribers.csv is empty or missing subscriber_id; fixture shape changed"
    )
    return [r["subscriber_id"] for r in rows]


@pytest.fixture(scope="module")
def hard_deleted_rows() -> list[dict[str, str]]:
    rows = read_manifest("hard_deleted_subscribers.csv")
    assert rows and {"subscriber_id", "hard_delete_at"} <= rows[0].keys(), (
        "hard_deleted_subscribers.csv is empty or missing expected columns; fixture shape changed"
    )
    return rows


def test_soft_deleted_subscribers_present_with_churned_terminal_status(
    trino_conn: Connection, soft_deleted_ids: list[str]
) -> None:
    """Every soft-deleted subscriber_id's current dim_subscriber row must
    show status = 'churned' (the gold remapping of source 'cancelled' and
    'deleted'), not disappear from the dimension and not retain the raw
    source vocabulary."""
    id_list = sql_in_list(soft_deleted_ids)

    present = scalar(
        trino_conn,
        f"select count(distinct subscriber_id) from iceberg.dev_dimensions.dim_subscriber "
        f"where subscriber_id in ({id_list})",
    )
    assert present == len(soft_deleted_ids), (
        f"expected all {len(soft_deleted_ids)} soft-deleted subscriber_ids present in "
        f"dim_subscriber, found {present}"
    )

    rows = fetch_all(
        trino_conn,
        f"select subscriber_id, status from iceberg.dev_dimensions.dim_subscriber "
        f"where is_current and subscriber_id in ({id_list}) "
        f"and status <> '{SOFT_DELETE_TERMINAL_STATUS}'",
    )
    assert not rows, (
        f"{len(rows)} soft-deleted subscriber(s) have a current status other than "
        f"'{SOFT_DELETE_TERMINAL_STATUS}', e.g. {rows[:5]}"
    )


@pytest.mark.parametrize(
    "fact_table, event_time_col",
    HARD_DELETE_TOUCHING_FACTS,
    ids=[f[0] for f in HARD_DELETE_TOUCHING_FACTS],
)
def test_hard_deleted_subscribers_have_no_fact_activity_past_cutoff(
    trino_conn: Connection,
    hard_deleted_rows: list[dict[str, str]],
    fact_table: str,
    event_time_col: str,
) -> None:
    """No row in {fact_table} for a hard-deleted subscriber may carry an
    event timestamp after that subscriber's hard_delete_at cutoff. Joins
    through dim_subscriber on subscriber_sk to subscriber_id (not a fixed
    subscriber_sk) deliberately: a hard-deleted subscriber can still have
    multiple historical dim_subscriber versions before their cutoff, and
    every one of those versions' facts must respect the same cutoff, not
    just the version active at deletion time."""
    values = ",\n".join(
        f"({sql_in_list([r['subscriber_id']])}, {sql_ts(r['hard_delete_at'])})"
        for r in hard_deleted_rows
    )
    violations = scalar(
        trino_conn,
        f"""
        select count(*)
        from iceberg.dev_facts.{fact_table} f
        join iceberg.dev_dimensions.dim_subscriber d on f.subscriber_sk = d.subscriber_sk
        join (values {values}) as h(subscriber_id, hard_delete_at)
            on d.subscriber_id = h.subscriber_id
        where f.{event_time_col} > h.hard_delete_at
        """,
    )
    assert violations == 0, (
        f"{violations} row(s) in {fact_table} carry a {event_time_col} after their "
        "subscriber's hard_delete_at cutoff"
    )
