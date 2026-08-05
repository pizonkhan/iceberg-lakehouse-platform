"""SCD interval integrity for dim_subscriber and dim_title.

modeling.md's "Required data tests on every SCD dim" declares: no interval
overlap and no gap between consecutive versions of one natural key (each
effective_to equals the next effective_from), and exactly one is_current
row per natural key. These already exist as dbt tests
(dbt_utils.mutually_exclusive_ranges and dbt_utils.unique_combination_of_columns
in _dim_subscriber.yml and the equivalent for dim_title); this file adds
the pytest-level equivalent for this integration suite's own completeness,
per the work package brief, and proves the detection logic itself works
with a scratch fail-then-fix demonstration rather than only asserting
zero against real data.

dim_subscriber's grain-uniqueness and is_current-uniqueness checks live in
test_grain_violations.py (the work package brief groups "the composite
grain (subscriber_id, effective_from) plus the is_current-uniqueness
invariant" together there for dim_subscriber specifically); this file
covers dim_subscriber's interval overlap/gap check plus both checks for
dim_title, which is not exercised anywhere else in this suite.
"""

from __future__ import annotations

from trino.dbapi import Connection

from tests.integration._helpers import scalar, sql_str
from tests.integration.conftest import ScratchTableFactory

DIM_SUBSCRIBER_COLUMNS = (
    "subscriber_sk, subscriber_id, email, display_name, country_code, acquisition_channel, "
    "signup_date, plan_tier, status, current_plan_tier, previous_plan_tier, churn_date_key"
)


def _interval_violation_count_sql(table: str, natural_key_col: str) -> str:
    """Mirrors dbt_utils.mutually_exclusive_ranges(gaps='not_allowed',
    zero_length_range_allowed=False): every row's interval must be
    non-empty (effective_from < effective_to), and every row except the
    last version of a natural key must have effective_to exactly equal to
    the next version's effective_from, no overlap and no gap either
    direction."""
    return f"""
        with ordered as (
            select
                {natural_key_col} as natural_key,
                effective_from,
                effective_to,
                lead(effective_from) over (
                    partition by {natural_key_col} order by effective_from
                ) as next_effective_from,
                row_number() over (
                    partition by {natural_key_col} order by effective_from desc
                ) = 1 as is_last
            from {table}
        )
        select count(*) from ordered
        where not (
            effective_from < effective_to
            and coalesce(effective_to = next_effective_from, is_last)
        )
    """


def _is_current_violation_count_sql(table: str, natural_key_col: str) -> str:
    return f"""
        select count(*) from (
            select {natural_key_col} as natural_key, count(*) as c
            from {table}
            where is_current
            group by {natural_key_col}
            having count(*) <> 1
        )
    """


def test_dim_subscriber_intervals_have_no_overlaps_or_gaps(trino_conn: Connection) -> None:
    """Every subscriber's version chain must be contiguous and non-overlapping:
    a point-in-time join relies on this to guarantee at most one match per
    event timestamp (modeling.md's "Because intervals are half-open,
    contiguous, and non-overlapping ... each of these joins matches at most
    one row")."""
    violations = scalar(
        trino_conn,
        _interval_violation_count_sql("iceberg.dev_dimensions.dim_subscriber", "subscriber_id"),
    )
    assert violations == 0, (
        f"found {violations} interval overlap/gap violation(s) in dim_subscriber"
    )


def test_dim_title_intervals_have_no_overlaps_or_gaps(trino_conn: Connection) -> None:
    """Same contiguity contract for dim_title, the other Type 2 SCD dim."""
    violations = scalar(
        trino_conn, _interval_violation_count_sql("iceberg.dev_dimensions.dim_title", "title_id")
    )
    assert violations == 0, f"found {violations} interval overlap/gap violation(s) in dim_title"


def test_dim_title_is_current_uniqueness_has_no_violations(trino_conn: Connection) -> None:
    """Exactly one is_current=true row per title_id."""
    violations = scalar(
        trino_conn, _is_current_violation_count_sql("iceberg.dev_dimensions.dim_title", "title_id")
    )
    assert violations == 0, f"found {violations} title_id(s) with != 1 is_current row"


def test_scd_interval_detection_has_teeth_on_scratch_overlap(
    trino_conn: Connection, scratch_table: ScratchTableFactory
) -> None:
    """Proves test_dim_subscriber_intervals_have_no_overlaps_or_gaps is not
    vacuously true. Copies one real subscriber's full version history (a
    subscriber chosen at runtime for having 2+ versions, never hardcoded)
    into iceberg.test_scratch, shifts the earliest version's effective_to
    forward by one hour so it no longer equals the next version's
    effective_from, reruns the identical detection query against the
    scratch copy, and asserts it now reports the violation. Any nonzero
    shift breaks the required "effective_to == next effective_from"
    equality regardless of direction, so this single corruption exercises
    the same boundary condition that would also catch a gap.
    """
    subscriber_id = scalar(
        trino_conn,
        "select subscriber_id from iceberg.dev_dimensions.dim_subscriber "
        "group by subscriber_id having count(*) >= 2 limit 1",
    )
    assert subscriber_id is not None, (
        "no subscriber with 2+ versions found; cannot construct this proof"
    )

    fq = scratch_table(
        "dim_subscriber_interval_overlap",
        f"""
        select
            {DIM_SUBSCRIBER_COLUMNS},
            effective_from,
            case when row_number() over (order by effective_from) = 1
                 then effective_to + interval '1' hour
                 else effective_to
            end as effective_to,
            is_current, scd_version, row_hash, is_inferred, loaded_at
        from iceberg.dev_dimensions.dim_subscriber
        where subscriber_id = {sql_str(subscriber_id)}
        """,
    )

    violations = scalar(trino_conn, _interval_violation_count_sql(fq, "subscriber_id"))
    assert violations > 0, (
        "expected the deliberately shifted effective_to to be detected as an interval "
        "violation, got 0; the detection query has no teeth"
    )


def test_scd_is_current_detection_has_teeth_on_scratch_duplicate(
    trino_conn: Connection, scratch_table: ScratchTableFactory
) -> None:
    """Same fail-then-fix pattern for the is_current-uniqueness invariant:
    copies one real subscriber's history, flips is_current to true on the
    second-most-recent version (which is genuinely false in the real
    table), reruns the detection query, and confirms it now reports two
    is_current rows for that subscriber_id.
    """
    subscriber_id = scalar(
        trino_conn,
        "select subscriber_id from iceberg.dev_dimensions.dim_subscriber "
        "group by subscriber_id having count(*) >= 2 limit 1",
    )
    assert subscriber_id is not None, (
        "no subscriber with 2+ versions found; cannot construct this proof"
    )

    fq = scratch_table(
        "dim_subscriber_is_current_duplicate",
        f"""
        select
            {DIM_SUBSCRIBER_COLUMNS},
            effective_from, effective_to,
            case when row_number() over (order by effective_from desc) = 2
                 then true
                 else is_current
            end as is_current,
            scd_version, row_hash, is_inferred, loaded_at
        from iceberg.dev_dimensions.dim_subscriber
        where subscriber_id = {sql_str(subscriber_id)}
        """,
    )

    violations = scalar(trino_conn, _is_current_violation_count_sql(fq, "subscriber_id"))
    assert violations == 1, (
        f"expected exactly 1 subscriber_id with a duplicated is_current flag, got {violations}; "
        "the detection query has no teeth"
    )
