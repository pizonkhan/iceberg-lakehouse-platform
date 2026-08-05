"""Referential integrity: every fact's foreign key must resolve to a real
row in its target dimension, never to nothing.

Two families of FK on the bus matrix (.notes/modeling.md "Bus matrix"),
each with its own resolution guarantee and therefore its own check:

- Hash surrogate keys (subscriber_sk, title_sk, device_sk, plan_sk,
  payment_method_sk): NOT NULL everywhere, resolved via coalesce() in
  every fact model to the target dimension's unknown member (surrogate
  key md5('-1')) whenever a real match is not found. "Never resolves to
  nothing" for these means the FK value must always equal some row's
  surrogate key in the target dimension, real or unknown.
- Date keys (session_date_key, billing_date_key, etc.): INTEGER YYYYMMDD
  values with no unknown-member row in dim_date; instead a handful of
  fct_signup_funnel milestone date keys are permitted NULL until that
  milestone occurs (modeling.md's single declared exception to the
  "FK columns are NOT NULL" rule). "Never resolves to nothing" for these
  means every non-null value must exist in dim_date.

Every check below uses a "distinct FK values, then join the dimension"
shape rather than a direct fact-to-dimension join: fct_playback_events and
fct_daily_subscription_snapshot are large enough (120M and 27M rows) that
a direct join risks the same per-node memory ceiling documented in
test_grain_violations.py, while the FK columns themselves only ever take
on as many distinct values as their target dimension has rows (at most a
few hundred thousand). Distinct-first bounds the join's memory to that
much smaller cardinality regardless of the fact table's own row count,
and is applied uniformly here (even to the small fact tables) so every
check in this file shares one reviewed query shape.
"""

from __future__ import annotations

import pytest
from trino.dbapi import Connection

from tests.integration._helpers import scalar

FactsSchema = "iceberg.dev_facts"
DimsSchema = "iceberg.dev_dimensions"


def _sk_fk_missing_count(
    trino_conn: Connection, fact_table: str, fk_col: str, dim_table: str, dim_pk_col: str
) -> int:
    sql = f"""
        select count(*) from (
            select distinct {fk_col} as fk
            from {FactsSchema}.{fact_table}
        ) f
        left join {DimsSchema}.{dim_table} d on f.fk = d.{dim_pk_col}
        where d.{dim_pk_col} is null
    """
    return int(scalar(trino_conn, sql))


def _date_fk_missing_count(
    trino_conn: Connection, fact_table: str, fk_col: str, nullable: bool
) -> int:
    null_filter = f"where {fk_col} is not null" if nullable else ""
    sql = f"""
        select count(*) from (
            select distinct {fk_col} as fk
            from {FactsSchema}.{fact_table}
            {null_filter}
        ) f
        left join {DimsSchema}.dim_date d on f.fk = d.date_key
        where d.date_key is null
    """
    return int(scalar(trino_conn, sql))


# (fact_table, fk_column, dim_table, dim_pk_column), one row per FK on the bus matrix.
SK_FK_CASES = [
    ("fct_playback_events", "subscriber_sk", "dim_subscriber", "subscriber_sk"),
    ("fct_playback_events", "title_sk", "dim_title", "title_sk"),
    ("fct_playback_events", "device_sk", "dim_device", "device_sk"),
    ("fct_billing_transactions", "subscriber_sk", "dim_subscriber", "subscriber_sk"),
    ("fct_billing_transactions", "plan_sk", "dim_plan", "plan_sk"),
    ("fct_billing_transactions", "payment_method_sk", "dim_payment_method", "payment_method_sk"),
    ("fct_daily_subscription_snapshot", "subscriber_sk", "dim_subscriber", "subscriber_sk"),
    ("fct_daily_subscription_snapshot", "plan_sk", "dim_plan", "plan_sk"),
    ("fct_signup_funnel", "subscriber_sk", "dim_subscriber", "subscriber_sk"),
    ("fct_signup_funnel", "plan_sk", "dim_plan", "plan_sk"),
    ("fct_watchlist_adds", "subscriber_sk", "dim_subscriber", "subscriber_sk"),
    ("fct_watchlist_adds", "title_sk", "dim_title", "title_sk"),
]


@pytest.mark.parametrize(
    "fact_table, fk_col, dim_table, dim_pk_col",
    SK_FK_CASES,
    ids=[f"{f}.{c}->{d}" for f, c, d, _ in SK_FK_CASES],
)
def test_surrogate_key_fk_resolves_to_real_or_unknown_member(
    trino_conn: Connection, fact_table: str, fk_col: str, dim_table: str, dim_pk_col: str
) -> None:
    """Every distinct value {fact_table}.{fk_col} takes must exist as a
    surrogate key in {dim_table}. Because every model resolves a miss via
    coalesce() to that dimension's unknown member row rather than leaving
    the column null or dropping the row, this single check (a value with
    no matching dimension row at all) is exactly the failure mode "resolves
    to nothing": a resolution to a real member and a resolution to the
    documented unknown member both pass, since the unknown member is
    itself a real row in the dimension table."""
    missing = _sk_fk_missing_count(trino_conn, fact_table, fk_col, dim_table, dim_pk_col)
    assert missing == 0, (
        f"{fact_table}.{fk_col} has {missing} distinct value(s) with no matching row in "
        f"{dim_table}.{dim_pk_col}"
    )


# (fact_table, fk_column, nullable). Nullable columns are the milestone date keys on
# fct_signup_funnel, modeling.md's single declared exception to "FK columns are NOT NULL".
DATE_FK_CASES = [
    ("fct_playback_events", "session_date_key", False),
    ("fct_billing_transactions", "billing_date_key", False),
    ("fct_daily_subscription_snapshot", "snapshot_date_key", False),
    ("fct_watchlist_adds", "added_date_key", False),
    ("fct_signup_funnel", "signup_date_key", False),
    ("fct_signup_funnel", "registered_date_key", True),
    ("fct_signup_funnel", "email_verified_date_key", True),
    ("fct_signup_funnel", "payment_method_added_date_key", True),
    ("fct_signup_funnel", "plan_selected_date_key", True),
    ("fct_signup_funnel", "first_stream_date_key", True),
]


@pytest.mark.parametrize(
    "fact_table, fk_col, nullable",
    DATE_FK_CASES,
    ids=[f"{f}.{c}" for f, c, _ in DATE_FK_CASES],
)
def test_date_key_fk_resolves_to_dim_date(
    trino_conn: Connection, fact_table: str, fk_col: str, nullable: bool
) -> None:
    """Every non-null value {fact_table}.{fk_col} takes must exist as
    date_key in dim_date. dim_date has no unknown-member row (unlike the
    hash-keyed dimensions): the "not yet occurred" case on
    fct_signup_funnel's milestone columns is represented by NULL instead,
    per modeling.md's explicit exception, so NULLs are excluded here
    rather than treated as a violation."""
    missing = _date_fk_missing_count(trino_conn, fact_table, fk_col, nullable)
    assert missing == 0, (
        f"{fact_table}.{fk_col} has {missing} distinct non-null value(s) with no matching "
        f"dim_date.date_key"
    )
