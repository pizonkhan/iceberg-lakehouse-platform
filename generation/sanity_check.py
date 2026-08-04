"""Query generation/output/ with duckdb and confirm every pathology is
actually present, rather than trusting that the generator code did what it
intended.

Usage:
    uv run python -m generation.sanity_check [output_dir]

Exits non-zero if any check fails. Intended to run against a --scale small
run before ever committing to --scale full.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

from generation.config import GENERATION_NOW, OUTPUT_ROOT

PASS = "PASS"
FAIL = "FAIL"


def _glob(output_dir: Path, entity: str) -> str:
    return str(output_dir / entity / "*.parquet")


def run_checks(output_dir: Path) -> bool:
    con = duckdb.connect()
    all_ok = True

    def check(name: str, condition: bool, detail: str) -> None:
        nonlocal all_ok
        status = PASS if condition else FAIL
        if not condition:
            all_ok = False
        print(f"[{status}] {name}: {detail}")

    # -- row counts -------------------------------------------------------
    print("\n-- row counts --")
    for entity in [
        "subscriber_events",
        "title_events",
        "plans",
        "devices",
        "title_genres",
        "playback_sessions",
        "billing_ledger",
        "watchlist_adds",
        "signup_funnel",
    ]:
        count = con.execute(
            f"select count(*) from read_parquet('{_glob(output_dir, entity)}')"
        ).fetchone()
        print(f"  {entity}: {count[0] if count else 0}")

    # -- pathology 1: late-arriving dimension members ----------------------
    late_ids_path = output_dir / "_pathology_manifest" / "late_arriving_subscribers.csv"
    if late_ids_path.exists() and late_ids_path.stat().st_size > 0:
        late_file_exists = (
            output_dir / "subscriber_events" / "part-00001-late-arrival.parquet"
        ).exists()
        referenced = con.execute(
            f"""
            select count(distinct s.subscriber_id) from read_csv('{late_ids_path}') s
            where exists (
                select 1 from read_parquet('{_glob(output_dir, "playback_sessions")}') p
                where p.subscriber_id = s.subscriber_id
            )
            """
        ).fetchone()
        referenced_count = referenced[0] if referenced else 0
        check(
            "pathology 1: late-arriving subscribers",
            late_file_exists and referenced_count > 0,
            f"late-arrival file present={late_file_exists}, "
            f"{referenced_count} late-arrival subscriber_ids referenced by playback facts",
        )

    # -- pathology 2: midstream join targets --------------------------------
    midstream_path = output_dir / "_pathology_manifest" / "midstream_join_targets.csv"
    if midstream_path.exists() and midstream_path.stat().st_size > 0:
        row = con.execute(f"select count(*) from read_csv('{midstream_path}')").fetchone()
        n_targets = row[0] if row else 0
        matched = con.execute(
            f"""
            select count(*) from read_csv('{midstream_path}') m
            where exists (
                select 1 from read_parquet('{_glob(output_dir, "playback_sessions")}') p
                where p.playback_session_id = m.constructed_id
                and p.session_started_at = m.event_timestamp::timestamp
            ) or exists (
                select 1 from read_parquet('{_glob(output_dir, "billing_ledger")}') b
                where b.billing_transaction_id = m.constructed_id
                and b.transaction_posted_at = m.event_timestamp::timestamp
            )
            """
        ).fetchone()
        matched_count = matched[0] if matched else 0
        detail = f"{matched_count}/{n_targets} rows found with a matching fact timestamp"
        ok = n_targets > 0 and matched_count == n_targets
        check("pathology 2: midstream join targets", ok, detail)

    # -- pathology 5: duplicate business keys -------------------------------
    dup_signup_path = output_dir / "_pathology_manifest" / "duplicate_signup_ids.csv"
    if dup_signup_path.exists() and dup_signup_path.stat().st_size > 0:
        row = con.execute(
            f"""
            select count(*) from (
                select signup_id, count(*) c
                from read_parquet('{_glob(output_dir, "signup_funnel")}')
                group by signup_id having count(*) > 1
            )
            """
        ).fetchone()
        dup_count = row[0] if row else 0
        check(
            "pathology 5: duplicate signup_id",
            dup_count > 0,
            f"{dup_count} signup_id values repeated",
        )

    dup_billing_path = output_dir / "_pathology_manifest" / "duplicate_billing_ids.csv"
    if dup_billing_path.exists() and dup_billing_path.stat().st_size > 0:
        row = con.execute(
            f"""
            select count(*) from (
                select billing_transaction_id, count(*) c
                from read_parquet('{_glob(output_dir, "billing_ledger")}')
                group by billing_transaction_id having count(*) > 1
            )
            """
        ).fetchone()
        dup_count = row[0] if row else 0
        check(
            "pathology 5: duplicate billing_transaction_id",
            dup_count > 0,
            f"{dup_count} billing_transaction_id values repeated",
        )

    # -- pathology 6: out-of-order playback arrival --------------------------
    row = con.execute(
        f"""
        with per_file as (
            select filename, min(session_started_at) mn, max(session_started_at) mx
            from read_parquet('{_glob(output_dir, "playback_sessions")}', filename = true)
            group by filename
        ),
        ordered as (
            select filename, mn, mx, row_number() over (order by filename) rn
            from per_file
        )
        select count(*) from ordered a join ordered b
        on b.rn = a.rn + 1 and b.mn < a.mn
        """
    ).fetchone()
    inversion_count = row[0] if row else 0
    check(
        "pathology 6: out-of-order arrival",
        inversion_count > 0,
        f"{inversion_count} consecutive-file pairs where the later file's min timestamp "
        "precedes the earlier file's min timestamp",
    )

    # -- pathology 7: null device_id -----------------------------------------
    playback_glob = _glob(output_dir, "playback_sessions")
    row = con.execute(
        f"select count(*) from read_parquet('{playback_glob}') where device_id is null"
    ).fetchone()
    null_device_count = row[0] if row else 0
    check(
        "pathology 7: null device_id",
        null_device_count > 0,
        f"{null_device_count} rows with null device_id",
    )

    # -- pathology 8: same-day multiple changes ------------------------------
    same_day_path = output_dir / "_pathology_manifest" / "same_day_change_subscribers.csv"
    if same_day_path.exists() and same_day_path.stat().st_size > 0:
        row = con.execute(
            f"""
            select count(distinct subscriber_id) from (
                select subscriber_id, change_timestamp::date d, count(*) c
                from read_parquet('{_glob(output_dir, "subscriber_events")}')
                where subscriber_id in (select subscriber_id from read_csv('{same_day_path}'))
                group by subscriber_id, d
                having count(*) >= 2
            )
            """
        ).fetchone()
        confirmed = row[0] if row else 0
        check(
            "pathology 8: same-day multiple changes",
            confirmed > 0,
            f"{confirmed} subscribers confirmed with 2+ change events on one calendar day",
        )

    # -- pathology 9: mid-stream schema drift --------------------------------
    # con.execute() returns the connection itself, not an independent cursor,
    # so each call's .description must be captured before the next execute()
    # overwrites it; two deferred reads off the same object would silently
    # both end up describing whichever query ran last.
    files = sorted((output_dir / "playback_sessions").glob("part-0*.parquet"))
    if len(files) >= 2:
        first_desc = con.execute(f"select * from read_parquet('{files[0]}') limit 0").description
        first_cols = {d[0] for d in (first_desc or [])}
        last_desc = con.execute(f"select * from read_parquet('{files[-1]}') limit 0").description
        last_cols = {d[0] for d in (last_desc or [])}
        check(
            "pathology 9: schema drift",
            ("playback_quality" not in first_cols) and ("playback_quality" in last_cols),
            f"playback_quality absent in {files[0].name}, present in {files[-1].name}",
        )

    # -- pathology 10: soft and hard deletes ---------------------------------
    soft_path = output_dir / "_pathology_manifest" / "soft_deleted_subscribers.csv"
    if soft_path.exists() and soft_path.stat().st_size > 0:
        subscriber_events_glob = _glob(output_dir, "subscriber_events")
        row = con.execute(
            f"""
            select count(distinct subscriber_id) from read_parquet('{subscriber_events_glob}')
            where subscriber_id in (select subscriber_id from read_csv('{soft_path}'))
            and status in ('cancelled', 'deleted')
            """
        ).fetchone()
        confirmed = row[0] if row else 0
        check(
            "pathology 10: soft-deleted subscribers carry a delete-signal status",
            confirmed > 0,
            f"{confirmed} soft-delete subscribers have a cancelled/deleted status event",
        )

    hard_path = output_dir / "_pathology_manifest" / "hard_deleted_subscribers.csv"
    if hard_path.exists() and hard_path.stat().st_size > 0:
        row = con.execute(
            f"""
            select count(*) from read_csv('{hard_path}') h
            where exists (
                select 1 from read_parquet('{_glob(output_dir, "playback_sessions")}') p
                where p.subscriber_id = h.subscriber_id
                and p.session_started_at > h.hard_delete_at::timestamp
            )
            """
        ).fetchone()
        violations = row[0] if row else 0
        detail = f"{violations} hard-delete subscribers have a playback event past hard_delete_at"
        check("pathology 10: hard-deleted subscribers stop appearing", violations == 0, detail)

    # -- pathology 11: malformed rows -----------------------------------------
    malformed_path = output_dir / "_pathology_manifest" / "malformed_playback_events.csv"
    if malformed_path.exists() and malformed_path.stat().st_size > 0:
        row = con.execute(
            f"""
            select
                sum(case when watch_duration_seconds < 0 then 1 else 0 end) neg_duration,
                sum(case when session_ended_at < session_started_at
                    then 1 else 0 end) ended_before_started,
                sum(case when session_started_at > timestamp '{GENERATION_NOW.isoformat()}'
                    then 1 else 0 end) future_ts
            from read_parquet('{_glob(output_dir, "playback_sessions")}')
            """
        ).fetchone()
        neg_duration, ended_before, future_ts = row if row else (0, 0, 0)
        detail = (
            f"negative_duration={neg_duration}, ended_before_started={ended_before}, "
            f"future_timestamp={future_ts}"
        )
        check(
            "pathology 11: malformed playback rows",
            (neg_duration or 0) > 0 and (ended_before or 0) > 0 and (future_ts or 0) > 0,
            detail,
        )

    return all_ok


def main() -> int:
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else OUTPUT_ROOT
    print(f"running sanity checks against {output_dir}")
    ok = run_checks(output_dir)
    print("\nALL CHECKS PASSED" if ok else "\nSOME CHECKS FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
