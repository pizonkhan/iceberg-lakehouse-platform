"""CLI entrypoint: generate the full synthetic source-data corpus.

Usage:
    uv run python -m generation.generate --scale small
    uv run python -m generation.generate --scale full

Writes everything under generation/output/ (gitignored). Run --scale small
first and validate with generation/sanity_check.py before ever running
--scale full: at full scale this writes ~120M playback rows and can take
several minutes.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import structlog

from generation import billing, playback, reference, signup_funnel, subscribers, titles, watchlist
from generation.config import RunConfig
from generation.rng import child_rng
from generation.writer import write_manifest_csv, write_manifest_json

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ]
)
log = structlog.get_logger()


def _reset_output_dir(output_root: Path) -> None:
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True)


def run(scale_name: str) -> None:
    run_config = RunConfig.for_scale(scale_name)
    output_root = run_config.output_root
    manifest_dir = output_root / "_pathology_manifest"

    log.info(
        "generation.start", scale=scale_name, seed=run_config.seed, output_root=str(output_root)
    )
    _reset_output_dir(output_root)

    timings: dict[str, float] = {}
    counts: dict[str, int] = {}

    # -- reference data -------------------------------------------------
    t0 = time.perf_counter()
    ref_rng = child_rng(run_config.seed, "reference.core")
    plans_table = reference.generate_plans(run_config.scale.plan_count, ref_rng)
    devices_table = reference.generate_devices(run_config.scale.device_count, ref_rng)
    (output_root / "plans").mkdir(parents=True, exist_ok=True)
    (output_root / "devices").mkdir(parents=True, exist_ok=True)
    pq.write_table(plans_table, output_root / "plans" / "part-00000.parquet", compression="zstd")
    pq.write_table(
        devices_table, output_root / "devices" / "part-00000.parquet", compression="zstd"
    )
    plan_ids = plans_table.column("plan_id").to_numpy(zero_copy_only=False)
    device_ids = devices_table.column("device_id").to_numpy(zero_copy_only=False)
    timings["reference"] = time.perf_counter() - t0
    counts["plans"] = plans_table.num_rows
    counts["devices"] = devices_table.num_rows
    log.info("generation.reference.done", plans=counts["plans"], devices=counts["devices"])

    # -- titles -----------------------------------------------------------
    t0 = time.perf_counter()
    title_result, title_events = titles.generate_titles(run_config)
    title_events_table = titles.events_to_table(title_events)
    (output_root / "title_events").mkdir(parents=True, exist_ok=True)
    pq.write_table(
        title_events_table, output_root / "title_events" / "part-00000.parquet", compression="zstd"
    )
    bridge_rng = child_rng(run_config.seed, "titles.genre_bridge")
    bridge_table = reference.generate_title_genre_bridge(list(title_result.title_ids), bridge_rng)
    (output_root / "title_genres").mkdir(parents=True, exist_ok=True)
    pq.write_table(
        bridge_table, output_root / "title_genres" / "part-00000.parquet", compression="zstd"
    )
    timings["titles"] = time.perf_counter() - t0
    counts["title_events"] = title_events_table.num_rows
    counts["title_genres"] = bridge_table.num_rows
    log.info(
        "generation.titles.done",
        title_events=counts["title_events"],
        title_genres=counts["title_genres"],
        seconds=round(timings["titles"], 2),
    )

    # -- subscribers --------------------------------------------------------
    t0 = time.perf_counter()
    subscriber_result, subscriber_events = subscribers.generate_subscribers(run_config)

    late_ids = subscriber_result.late_arrival_ids
    normal_events = [e for e in subscriber_events if e["subscriber_id"] not in late_ids]
    late_events = [e for e in subscriber_events if e["subscriber_id"] in late_ids]

    (output_root / "subscriber_events").mkdir(parents=True, exist_ok=True)
    normal_table = subscribers.events_to_table(normal_events)
    pq.write_table(
        normal_table, output_root / "subscriber_events" / "part-00000.parquet", compression="zstd"
    )
    if late_events:
        # pathology 1: written as the physically last file so any fact
        # event already referencing this subscriber_id lands in an
        # earlier-written file than the dimension's own signup event.
        late_table = subscribers.events_to_table(late_events)
        pq.write_table(
            late_table,
            output_root / "subscriber_events" / "part-00001-late-arrival.parquet",
            compression="zstd",
        )
    timings["subscribers"] = time.perf_counter() - t0
    counts["subscriber_events"] = len(subscriber_events)
    log.info(
        "generation.subscribers.done",
        subscribers=run_config.scale.subscriber_count,
        events=counts["subscriber_events"],
        late_arrival_events=len(late_events),
        seconds=round(timings["subscribers"], 2),
    )

    # -- pathology manifests: subscriber-side --------------------------------
    write_manifest_csv(
        [{"subscriber_id": sid} for sid in sorted(subscriber_result.late_arrival_ids)],
        manifest_dir / "late_arriving_subscribers.csv",
    )
    write_manifest_csv(
        [
            {"subscriber_id": sid, "hard_delete_at": str(ts)}
            for sid, ts in sorted(subscriber_result.hard_delete_at_by_id.items())
        ],
        manifest_dir / "hard_deleted_subscribers.csv",
    )
    write_manifest_csv(
        [{"subscriber_id": sid} for sid in sorted(subscriber_result.soft_delete_ids)],
        manifest_dir / "soft_deleted_subscribers.csv",
    )
    write_manifest_csv(
        [{"subscriber_id": sid} for sid in sorted(subscriber_result.same_day_ids)],
        manifest_dir / "same_day_change_subscribers.csv",
    )

    # -- facts ----------------------------------------------------------------
    t0 = time.perf_counter()
    playback_result = playback.generate_playback_events(
        run_config, subscriber_result, title_result, device_ids, output_root / "playback_sessions"
    )
    timings["playback"] = time.perf_counter() - t0
    counts["playback_sessions"] = playback_result["rows_written"]
    log.info(
        "generation.playback.done",
        rows=counts["playback_sessions"],
        batches=playback_result["num_batches"],
        seconds=round(timings["playback"], 2),
    )

    t0 = time.perf_counter()
    billing_result = billing.generate_billing_events(
        run_config, subscriber_result, plan_ids, output_root / "billing_ledger"
    )
    timings["billing"] = time.perf_counter() - t0
    counts["billing_ledger"] = billing_result["rows_written"]
    log.info(
        "generation.billing.done",
        rows=counts["billing_ledger"],
        seconds=round(timings["billing"], 2),
    )

    t0 = time.perf_counter()
    watchlist_result = watchlist.generate_watchlist_events(
        run_config, subscriber_result, title_result, output_root / "watchlist_adds"
    )
    timings["watchlist"] = time.perf_counter() - t0
    counts["watchlist_adds"] = watchlist_result["rows_written"]
    log.info(
        "generation.watchlist.done",
        rows=counts["watchlist_adds"],
        seconds=round(timings["watchlist"], 2),
    )

    t0 = time.perf_counter()
    funnel_result = signup_funnel.generate_signup_funnel(
        run_config, subscriber_result, plan_ids, output_root / "signup_funnel"
    )
    timings["signup_funnel"] = time.perf_counter() - t0
    counts["signup_funnel"] = funnel_result["rows_written"]
    log.info(
        "generation.signup_funnel.done",
        rows=counts["signup_funnel"],
        seconds=round(timings["signup_funnel"], 2),
    )

    # -- pathology manifests: fact-side --------------------------------------
    write_manifest_csv(
        playback_result["malformed_manifest"], manifest_dir / "malformed_playback_events.csv"
    )
    midstream_all: list[dict[str, Any]] = [
        *playback_result["midstream_manifest"],
        *billing_result["midstream_manifest"],
    ]
    write_manifest_csv(midstream_all, manifest_dir / "midstream_join_targets.csv")
    write_manifest_csv(billing_result["duplicate_ids"], manifest_dir / "duplicate_billing_ids.csv")
    write_manifest_csv(funnel_result["duplicate_ids"], manifest_dir / "duplicate_signup_ids.csv")
    write_manifest_json(
        {
            "cutoff_batch": playback_result["cutoff_batch"],
            "cutoff_boundary_at": playback_result["cutoff_boundary_at"],
            "note": "playback_sessions batches before cutoff_batch have no playback_quality "
            "column at all (different Parquet schema); batches at or after it do.",
        },
        manifest_dir / "schema_drift_cutoff.json",
    )
    write_manifest_json(
        {
            "fraction": run_config.pathology.out_of_order_fraction,
            "delay_batch_range": [
                run_config.pathology.out_of_order_delay_batches_min,
                run_config.pathology.out_of_order_delay_batches_max,
            ],
            "straggler_rows": playback_result["straggler_rows"],
            "mechanism": "playback_sessions batches cover chronological time slices; a fraction "
            "of each batch's rows is removed and re-injected into a later-numbered batch file, "
            "so file-write order stops tracking session_started_at order for those rows.",
        },
        manifest_dir / "out_of_order_summary.json",
    )

    total_seconds = sum(timings.values())
    write_manifest_json(
        {
            "scale": scale_name,
            "seed": run_config.seed,
            "counts": counts,
            "timings_seconds": {k: round(v, 2) for k, v in timings.items()},
            "total_seconds": round(total_seconds, 2),
        },
        output_root / "run_summary.json",
    )

    log.info("generation.complete", total_seconds=round(total_seconds, 2), counts=counts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", choices=["small", "full"], required=True)
    args = parser.parse_args()
    run(args.scale)


if __name__ == "__main__":
    main()
    sys.exit(0)
