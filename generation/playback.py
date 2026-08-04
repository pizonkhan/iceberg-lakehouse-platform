"""fct_playback_events source: ~120M completed playback sessions.

This is the dominant-volume, performance-sensitive generator in the
package: everything here is numpy array construction plus a handful of
pyarrow table/filter/concat calls, batched to keep peak memory bounded
regardless of total row count. No row-by-row Python loop touches the
per-session data.

Five of the eleven required pathologies live here:
- 2 (partial): late-arriving facts against a historical dim version, the
  playback share of the pair with billing.py.
- 6: out-of-order arrival, via a delayed-release pool that removes a
  fraction of each chronological batch's rows and re-injects them into a
  later-written batch, so file-write order stops tracking event-time order
  for that fraction.
- 7: null device_id.
- 9: mid-stream schema drift, playback_quality appears only in batches at
  or after a cutoff batch index, so early and late Parquet files literally
  have different schemas.
- 11: malformed rows (negative duration, ended before started, future
  timestamp).
"""

from __future__ import annotations

import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pyarrow as pa

from generation.config import RunConfig
from generation.ids import WeightedPool, sequential_ids
from generation.rng import child_rng
from generation.timeutil import from_epoch_s, to_epoch_s
from generation.schemas import (
    PlaybackGenerationSummary,
    SubscriberGenerationResult,
    TitleGenerationResult,
)
from generation.writer import write_batch

BITRATE_LADDER = np.array([800, 1200, 2500, 4000, 6000, 8000], dtype=np.int32)
PLAYBACK_QUALITIES = np.array(["sd", "hd", "fhd", "uhd"])
PLAYBACK_QUALITY_WEIGHTS = np.array([0.15, 0.35, 0.30, 0.20])

MALFORMED_NEGATIVE_DURATION = "negative_duration"
MALFORMED_ENDED_BEFORE_STARTED = "ended_before_started"
MALFORMED_FUTURE_TIMESTAMP = "future_timestamp"


def _epoch_array_to_timestamp(epoch_s: npt.NDArray[np.int64]) -> pa.Array:
    return pa.array(epoch_s.astype("datetime64[s]"))


def _build_batch_arrays(
    rng: np.random.Generator,
    n: int,
    slice_start_s: int,
    slice_end_s: int,
    subscriber_result: SubscriberGenerationResult,
    subscriber_pool: WeightedPool,
    title_pool: WeightedPool,
    device_ids: npt.NDArray[np.str_],
    session_id_start: int,
    null_device_fraction: float,
) -> dict[str, npt.NDArray[Any]]:
    sub_idx = subscriber_pool.sample_indices(n, rng)
    lower = subscriber_result.signup_epoch_s[sub_idx]
    upper = subscriber_result.upper_bound_epoch_s[sub_idx]
    lo = np.maximum(lower, slice_start_s)
    hi = np.minimum(upper, slice_end_s)
    # a hard-deleted subscriber's valid window can end entirely before this
    # batch's chronological slice starts (they were sampled into a slice
    # they were never active in). Falling back to their own [lower, upper]
    # window keeps every generated timestamp inside their true valid range;
    # widening hi up to lo + 1 unconditionally would instead invent a
    # timestamp inside this slice, silently violating hard-delete pathology
    # 10. The remaining max(..., lo + 1) only guards a genuine zero-width
    # window (upper == lower for a subscriber deleted the instant they
    # signed up).
    no_overlap = lo >= hi
    lo = np.where(no_overlap, lower, lo)
    hi = np.where(no_overlap, upper, hi)
    hi = np.maximum(hi, lo + 1)

    u = rng.random(n)
    started_epoch = (lo + (u * (hi - lo))).astype(np.int64)

    title_idx = title_pool.sample_indices(n, rng)
    device_idx = rng.integers(0, len(device_ids), n)

    duration = np.clip(rng.lognormal(mean=7.4, sigma=0.9, size=n), 30, 4 * 3600).astype(np.int64)
    ended_epoch = started_epoch + duration

    playback_percent = np.clip(rng.beta(5, 2, n), 0.0, 1.0).astype(np.float32)
    buffering_count = rng.poisson(1.2, n).astype(np.int32)
    avg_bitrate = rng.choice(BITRATE_LADDER, n)

    device_id = device_ids[device_idx]
    null_device_mask = rng.random(n) < null_device_fraction
    device_id = device_id.astype(object)
    device_id[null_device_mask] = None

    session_id = sequential_ids("pb_", session_id_start, n, 12)
    subscriber_id = subscriber_result.subscriber_ids[sub_idx]

    return {
        "playback_session_id": session_id,
        "subscriber_id": subscriber_id,
        "title_id": title_pool.ids[title_idx],
        "device_id": device_id,
        "session_started_at": started_epoch,
        "session_ended_at": ended_epoch,
        "watch_duration_seconds": duration,
        "playback_percent": playback_percent,
        "buffering_count": buffering_count,
        "avg_bitrate_kbps": avg_bitrate,
    }


def _apply_malformed_rows(
    rng: np.random.Generator,
    arrays: dict[str, npt.NDArray[Any]],
    fraction: float,
    now_epoch_s: int,
    date_dim_upper_epoch_s: int,
    hard_delete_ids: set[str],
) -> list[dict[str, str]]:
    n = len(arrays["playback_session_id"])
    malformed_mask = rng.random(n) < fraction
    malformed_idx = np.flatnonzero(malformed_mask)
    if len(malformed_idx) == 0:
        return []

    kind = rng.integers(0, 3, size=len(malformed_idx))
    manifest_rows: list[dict[str, str]] = []

    started = arrays["session_started_at"]
    ended = arrays["session_ended_at"]
    duration = arrays["watch_duration_seconds"]
    subscriber_id_col = arrays["subscriber_id"]

    for offset, row_idx in enumerate(malformed_idx):
        k = kind[offset]
        session_id = str(arrays["playback_session_id"][row_idx])
        # pathology 10 (hard delete) needs "this subscriber_id never
        # appears past hard_delete_at" to hold unambiguously, so the
        # future-timestamp flavor of pathology 11 is not applied to their
        # rows; it would otherwise silently plant a post-cutoff row for a
        # subscriber that pathology 10's own manifest promises has none.
        if k == 2 and str(subscriber_id_col[row_idx]) in hard_delete_ids:
            k = 0
        if k == 0:
            duration[row_idx] = -abs(int(duration[row_idx]))
            malformation = MALFORMED_NEGATIVE_DURATION
        elif k == 1:
            offset_s = int(rng.integers(60, 3600))
            ended[row_idx] = started[row_idx] - offset_s
            malformation = MALFORMED_ENDED_BEFORE_STARTED
        else:
            future_offset_days = int(rng.integers(1, 300))
            new_start = now_epoch_s + future_offset_days * 86400
            new_start = min(new_start, date_dim_upper_epoch_s - 7200)
            started[row_idx] = new_start
            ended[row_idx] = new_start + max(int(duration[row_idx]), 60)
            malformation = MALFORMED_FUTURE_TIMESTAMP
        manifest_rows.append({"playback_session_id": session_id, "malformation_type": malformation})

    return manifest_rows


def _table_from_arrays(
    arrays: dict[str, npt.NDArray[Any]], include_quality: bool, rng: np.random.Generator
) -> pa.Table:
    n = len(arrays["playback_session_id"])
    columns: dict[str, pa.Array] = {
        "playback_session_id": pa.array(arrays["playback_session_id"]),
        "subscriber_id": pa.array(arrays["subscriber_id"]),
        "title_id": pa.array(arrays["title_id"]),
        "device_id": pa.array(arrays["device_id"], type=pa.string()),
        "session_started_at": _epoch_array_to_timestamp(arrays["session_started_at"]),
        "session_ended_at": _epoch_array_to_timestamp(arrays["session_ended_at"]),
        "watch_duration_seconds": pa.array(arrays["watch_duration_seconds"].astype(np.int64)),
        "playback_percent": pa.array(arrays["playback_percent"]),
        "buffering_count": pa.array(arrays["buffering_count"]),
        "avg_bitrate_kbps": pa.array(arrays["avg_bitrate_kbps"]),
    }
    if include_quality:
        quality_idx = rng.choice(len(PLAYBACK_QUALITIES), size=n, p=PLAYBACK_QUALITY_WEIGHTS)
        columns["playback_quality"] = pa.array(PLAYBACK_QUALITIES[quality_idx])
    return pa.table(columns)


def generate_playback_events(
    run: RunConfig,
    subscriber_result: SubscriberGenerationResult,
    title_result: TitleGenerationResult,
    device_ids: npt.NDArray[np.str_],
    output_dir: Path,
) -> PlaybackGenerationSummary:
    scale = run.scale
    pathology = run.pathology

    subscriber_pool = WeightedPool.build(
        subscriber_result.subscriber_ids,
        child_rng(run.seed, "playback.weights.subscriber"),
        skew=0.8,
    )
    title_pool = WeightedPool.build(
        title_result.title_ids, child_rng(run.seed, "playback.weights.title"), skew=1.2
    )

    total_rows = scale.playback_event_count
    batch_size = scale.playback_batch_size
    num_batches = math.ceil(total_rows / batch_size)

    now_epoch_s = to_epoch_s(run.now)
    launch_epoch_s = to_epoch_s(run.platform_launch)
    date_dim_upper_epoch_s = to_epoch_s(run.date_dim_upper_bound)
    slice_width = (now_epoch_s - launch_epoch_s) / num_batches

    cutoff_batch = int(pathology.schema_drift_cutoff_fraction * num_batches)
    cutoff_boundary_epoch_s = int(launch_epoch_s + cutoff_batch * slice_width)

    delayed_pool: dict[int, list[pa.Table]] = defaultdict(list)
    malformed_manifest: list[dict[str, str]] = []
    session_id_cursor = 0
    rows_written = 0
    straggler_rows = 0

    for i in range(num_batches):
        n = min(batch_size, total_rows - i * batch_size)
        if n <= 0:
            break
        rng = child_rng(run.seed, f"playback.batch.{i}")

        slice_start_s = int(launch_epoch_s + i * slice_width)
        slice_end_s = int(launch_epoch_s + (i + 1) * slice_width)

        arrays = _build_batch_arrays(
            rng=rng,
            n=n,
            slice_start_s=slice_start_s,
            slice_end_s=slice_end_s,
            subscriber_result=subscriber_result,
            subscriber_pool=subscriber_pool,
            title_pool=title_pool,
            device_ids=device_ids,
            session_id_start=session_id_cursor,
            null_device_fraction=pathology.null_device_id_fraction,
        )
        session_id_cursor += n

        malformed_manifest.extend(
            _apply_malformed_rows(
                rng,
                arrays,
                pathology.malformed_playback_fraction,
                now_epoch_s,
                date_dim_upper_epoch_s,
                subscriber_result.hard_delete_ids,
            )
        )

        include_quality = i >= cutoff_batch
        table_i = _table_from_arrays(arrays, include_quality, rng)

        # pathology 6: out-of-order arrival. Peel off a fraction of this
        # batch's rows and stash them for release into a later batch, so
        # the file that eventually contains them is written well after
        # files containing chronologically later events.
        if i < num_batches - 1:
            straggler_mask = rng.random(n) < pathology.out_of_order_fraction
            if straggler_mask.any():
                straggler_table = table_i.filter(pa.array(straggler_mask))
                table_i = table_i.filter(pa.array(~straggler_mask))
                offsets = rng.integers(
                    pathology.out_of_order_delay_batches_min,
                    pathology.out_of_order_delay_batches_max + 1,
                    size=straggler_table.num_rows,
                )
                targets = np.minimum(i + offsets, num_batches - 1)
                for t in np.unique(targets):
                    subset = straggler_table.filter(pa.array(targets == t))
                    delayed_pool[int(t)].append(subset)
                straggler_rows += straggler_table.num_rows

        incoming = delayed_pool.pop(i, [])
        if incoming:
            if include_quality:
                incoming = [
                    tbl
                    if "playback_quality" in tbl.column_names
                    else tbl.append_column(
                        "playback_quality", pa.array([None] * tbl.num_rows, type=pa.string())
                    )
                    for tbl in incoming
                ]
            else:
                incoming = [
                    tbl.drop_columns(["playback_quality"])
                    if "playback_quality" in tbl.column_names
                    else tbl
                    for tbl in incoming
                ]
            table_i = pa.concat_tables([table_i, *incoming])

        write_batch(table_i, output_dir, i)
        rows_written += table_i.num_rows

    # any stragglers still pending release past the last batch land in the
    # last file rather than being dropped
    if delayed_pool:
        leftovers = [t for tables in delayed_pool.values() for t in tables]
        if leftovers:
            leftovers = [
                tbl
                if "playback_quality" in tbl.column_names
                else tbl.append_column(
                    "playback_quality", pa.array([None] * tbl.num_rows, type=pa.string())
                )
                for tbl in leftovers
            ]
            extra_table = pa.concat_tables(leftovers)
            write_batch(extra_table, output_dir, num_batches)
            rows_written += extra_table.num_rows

    midstream_manifest = _write_midstream_playback_targets(
        run,
        subscriber_result,
        title_result.title_ids,
        device_ids,
        output_dir,
        num_batches + 1,
        session_id_cursor,
    )

    return {
        "rows_written": rows_written,
        "num_batches": num_batches,
        "cutoff_batch": cutoff_batch,
        "cutoff_boundary_at": str(from_epoch_s(cutoff_boundary_epoch_s)),
        "straggler_rows": straggler_rows,
        "malformed_manifest": malformed_manifest,
        "midstream_manifest": midstream_manifest,
    }


def _write_midstream_playback_targets(
    run: RunConfig,
    subscriber_result: SubscriberGenerationResult,
    title_ids: npt.NDArray[np.str_],
    device_ids: npt.NDArray[np.str_],
    output_dir: Path,
    part_index: int,
    session_id_start: int,
) -> list[dict[str, str]]:
    """Pathology 2, playback half: construct events timestamped strictly
    inside a known (older_version, next_version) gap for a subscriber that
    has a still-newer version after that gap, so a correct point-in-time
    join must resolve to the older version, never the newest.
    """
    pathology = run.pathology
    rng = child_rng(run.seed, "playback.pathology.midstream")

    candidates = subscriber_result.midstream_candidates
    count = min(pathology.midstream_playback_target_count, len(candidates))
    if count == 0:
        return []
    chosen_idx = rng.choice(len(candidates), size=count, replace=False)

    manifest: list[dict[str, str]] = []
    rows = []
    chosen_title_idx = rng.integers(0, len(title_ids), size=count)
    for offset, ci in enumerate(chosen_idx):
        candidate = candidates[ci]
        gap_start_s = to_epoch_s(candidate.gap_start.astype("datetime64[s]").astype(object))
        gap_end_s = to_epoch_s(candidate.gap_end.astype("datetime64[s]").astype(object))
        span = max(gap_end_s - gap_start_s, 2)
        event_s = gap_start_s + int(rng.uniform(0.1, 0.9) * span)
        session_id = f"pb_{session_id_start + offset:012d}"
        duration = int(np.clip(rng.lognormal(7.2, 0.8), 30, 4 * 3600))
        rows.append(
            {
                "playback_session_id": session_id,
                "subscriber_id": candidate.subscriber_id,
                "title_id": str(title_ids[chosen_title_idx[offset]]),
                "device_id": str(device_ids[int(rng.integers(0, len(device_ids)))]),
                "session_started_at": event_s,
                "session_ended_at": event_s + duration,
                "watch_duration_seconds": duration,
                "playback_percent": float(np.clip(rng.beta(5, 2), 0, 1)),
                "buffering_count": int(rng.poisson(1.2)),
                "avg_bitrate_kbps": int(rng.choice(BITRATE_LADDER)),
                "playback_quality": str(rng.choice(PLAYBACK_QUALITIES, p=PLAYBACK_QUALITY_WEIGHTS)),
            }
        )
        manifest.append(
            {
                "fact_type": "playback",
                "constructed_id": session_id,
                "subscriber_id": candidate.subscriber_id,
                "event_timestamp": str(from_epoch_s(event_s)),
                "older_version_effective_from": str(candidate.gap_start),
                "older_version_effective_to": str(candidate.gap_end),
                "newer_version_effective_from": str(candidate.newer_version_start),
            }
        )

    table = pa.table(
        {
            "playback_session_id": pa.array([r["playback_session_id"] for r in rows]),
            "subscriber_id": pa.array([r["subscriber_id"] for r in rows]),
            "title_id": pa.array([r["title_id"] for r in rows]),
            "device_id": pa.array([r["device_id"] for r in rows]),
            "session_started_at": pa.array(
                np.array([r["session_started_at"] for r in rows], dtype=np.int64).astype(
                    "datetime64[s]"
                )
            ),
            "session_ended_at": pa.array(
                np.array([r["session_ended_at"] for r in rows], dtype=np.int64).astype(
                    "datetime64[s]"
                )
            ),
            "watch_duration_seconds": pa.array([r["watch_duration_seconds"] for r in rows]),
            "playback_percent": pa.array([r["playback_percent"] for r in rows], type=pa.float32()),
            "buffering_count": pa.array([r["buffering_count"] for r in rows]),
            "avg_bitrate_kbps": pa.array([r["avg_bitrate_kbps"] for r in rows]),
            "playback_quality": pa.array([r["playback_quality"] for r in rows]),
        }
    )
    write_batch(table, output_dir, part_index, prefix="part-pathology-midstream")
    return manifest
