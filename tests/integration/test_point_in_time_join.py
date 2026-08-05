"""Point-in-time correctness: the single most important test in this project.

modeling.md's "Point-in-time join rule" requires every fact touching an SCD
dimension to resolve the dimension row whose [effective_from, effective_to)
interval contains the fact's own event timestamp, never the dimension's
current version. Get this wrong and every downstream metric that slices
historical activity by a subscriber's plan_tier or status is silently
attributed to the wrong point in that subscriber's history.

Fixture: generation/output/_pathology_manifest/midstream_join_targets.csv,
300 fact_type=playback rows and 100 fact_type=billing rows (verified by
reading the file, not assumed). generation/playback.py's
_write_midstream_playback_targets and generation/billing.py's
_write_midstream_billing_targets deliberately timestamp each constructed
event inside a subscriber's older-version gap while that subscriber also
has a still-newer version, specifically so a join using "current version"
would resolve the row to the wrong key.

Ground truth in these tests is computed independently, not read off the
manifest's own older_version_effective_from/effective_to columns: those
columns record the raw pre-collapse subscriber_events change timestamps at
generation time, and dim_subscriber's build collapses consecutive events
with an unchanged (plan_tier, status) hash into one version (see
dim_subscriber.sql), so the manifest's recorded gap boundaries can differ
slightly from the actual built version boundaries. Each test instead
queries dim_subscriber directly for the version whose real interval
contains the fact's event_timestamp, which is the only definition of
"correct" that matters.
"""

from __future__ import annotations

from trino.dbapi import Connection

from tests.integration._helpers import fetch_all, read_manifest, sql_in_list, sql_str, sql_ts

Row = tuple[
    str, str, str, str, str | None
]  # constructed_id, subscriber_id, expected_sk, current_sk, actual_sk


def _resolved_vs_expected(
    trino_conn: Connection, fact_type: str, fact_table: str, id_column: str
) -> tuple[list[Row], int]:
    rows = [r for r in read_manifest("midstream_join_targets.csv") if r["fact_type"] == fact_type]
    assert rows, (
        f"midstream_join_targets.csv has no fact_type={fact_type!r} rows; "
        "the fixture's shape may have changed, re-check its columns before trusting this test"
    )

    manifest_values = ",\n".join(
        f"({sql_str(r['subscriber_id'])}, {sql_str(r['constructed_id'])}, "
        f"{sql_ts(r['event_timestamp'])})"
        for r in rows
    )
    constructed_ids = sql_in_list([r["constructed_id"] for r in rows])

    sql = f"""
    with manifest as (
        select * from (values
            {manifest_values}
        ) as t(subscriber_id, constructed_id, event_timestamp)
    ),
    -- ground truth: the dim_subscriber version whose real interval
    -- contains this fact's event timestamp, computed fresh here rather
    -- than trusted from the manifest's own recorded gap boundaries.
    expected as (
        select m.constructed_id, m.subscriber_id, d.subscriber_sk as expected_sk
        from manifest m
        join iceberg.dev_dimensions.dim_subscriber d
            on d.subscriber_id = m.subscriber_id
            and m.event_timestamp >= d.effective_from
            and m.event_timestamp < d.effective_to
    ),
    current_version as (
        select subscriber_id, subscriber_sk as current_sk
        from iceberg.dev_dimensions.dim_subscriber
        where is_current
    ),
    actual as (
        select {id_column} as constructed_id, subscriber_sk as actual_sk
        from iceberg.dev_facts.{fact_table}
        where {id_column} in ({constructed_ids})
    )
    select e.constructed_id, e.subscriber_id, e.expected_sk, c.current_sk, a.actual_sk
    from expected e
    join current_version c on c.subscriber_id = e.subscriber_id
    left join actual a on a.constructed_id = e.constructed_id
    """
    return fetch_all(trino_conn, sql), len(rows)


def test_playback_resolves_point_in_time_version_not_current(trino_conn: Connection) -> None:
    """Every midstream playback manifest row must resolve fct_playback_events.
    subscriber_sk to the dim_subscriber version whose interval genuinely
    contains session_started_at. A join keyed on "current version" would
    fail this for every row where the two differ."""
    results, expected_count = _resolved_vs_expected(
        trino_conn, "playback", "fct_playback_events", "playback_session_id"
    )
    assert len(results) == expected_count, (
        f"expected {expected_count} manifest rows to have a matching dim_subscriber interval "
        f"and a matching fct_playback_events row, got {len(results)}"
    )
    mismatches = [r for r in results if r[2] != r[4]]
    assert not mismatches, (
        f"resolved subscriber_sk did not match the point-in-time version for "
        f"{len(mismatches)} row(s), e.g. {mismatches[:5]}"
    )


def test_playback_point_in_time_resolution_has_teeth(trino_conn: Connection) -> None:
    """Proves the previous test is not vacuously true. Restricted to manifest
    rows where the correct (older) version genuinely differs from the
    subscriber's current version, this asserts the fact's resolved key also
    differs from the current version's key. A join that silently resolved
    to "current version" instead of "point in time version" would produce a
    real, non-null key for every row (passing a naive "some key got
    assigned" check) while failing this assertion on every single one."""
    results, _ = _resolved_vs_expected(
        trino_conn, "playback", "fct_playback_events", "playback_session_id"
    )
    genuinely_midstream = [r for r in results if r[2] != r[3]]
    assert genuinely_midstream, (
        "no manifest rows have an expected version differing from the current version; "
        "the fixture no longer exercises midstream resolution, this test would be vacuous"
    )
    resolved_to_current_instead = [r for r in genuinely_midstream if r[4] == r[3]]
    assert not resolved_to_current_instead, (
        f"{len(resolved_to_current_instead)} row(s) resolved to the CURRENT version's key "
        f"instead of the older point-in-time version, e.g. {resolved_to_current_instead[:5]}"
    )


def test_billing_resolves_point_in_time_version_not_current(trino_conn: Connection) -> None:
    """Same contract as playback, for fct_billing_transactions.subscriber_sk
    against transaction_posted_at, using the fact_type=billing half of the
    same manifest."""
    results, expected_count = _resolved_vs_expected(
        trino_conn, "billing", "fct_billing_transactions", "billing_transaction_id"
    )
    assert len(results) == expected_count, (
        f"expected {expected_count} manifest rows to have a matching dim_subscriber interval "
        f"and a matching fct_billing_transactions row, got {len(results)}"
    )
    mismatches = [r for r in results if r[2] != r[4]]
    assert not mismatches, (
        f"resolved subscriber_sk did not match the point-in-time version for "
        f"{len(mismatches)} row(s), e.g. {mismatches[:5]}"
    )


def test_billing_point_in_time_resolution_has_teeth(trino_conn: Connection) -> None:
    """Billing counterpart of test_playback_point_in_time_resolution_has_teeth:
    proves the billing correctness test would catch a "resolved to current
    version" bug rather than only proving a key of some kind was assigned."""
    results, _ = _resolved_vs_expected(
        trino_conn, "billing", "fct_billing_transactions", "billing_transaction_id"
    )
    genuinely_midstream = [r for r in results if r[2] != r[3]]
    assert genuinely_midstream, (
        "no manifest rows have an expected version differing from the current version; "
        "the fixture no longer exercises midstream resolution, this test would be vacuous"
    )
    resolved_to_current_instead = [r for r in genuinely_midstream if r[4] == r[3]]
    assert not resolved_to_current_instead, (
        f"{len(resolved_to_current_instead)} row(s) resolved to the CURRENT version's key "
        f"instead of the older point-in-time version, e.g. {resolved_to_current_instead[:5]}"
    )
