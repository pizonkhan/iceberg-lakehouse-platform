"""fct_watchlist_adds source: ~0.75M watchlist-add events.

No dedicated pathology construction happens here; it carries pathology 1
(late-arriving dimension members) incidentally, the same way billing and
playback do, because subscriber_id is sampled from the same pool that
includes the late-arrival subscriber_ids (see subscribers.py). Their own
signup events are simply written last in the subscriber output; nothing
watchlist-specific is required to make that pathology reachable here.
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pyarrow as pa

from generation.config import RunConfig
from generation.ids import WeightedPool, sequential_ids
from generation.rng import child_rng
from generation.types import (
    SubscriberGenerationResult,
    TitleGenerationResult,
    WatchlistGenerationSummary,
)
from generation.writer import write_batch


def _build_batch(
    rng: np.random.Generator,
    n: int,
    subscriber_result: SubscriberGenerationResult,
    subscriber_pool: WeightedPool,
    title_pool: WeightedPool,
    event_id_start: int,
) -> pa.Table:
    sub_idx = subscriber_pool.sample_indices(n, rng)
    lower = subscriber_result.signup_epoch_s[sub_idx]
    upper = subscriber_result.upper_bound_epoch_s[sub_idx]
    upper = np.maximum(upper, lower + 1)
    u = rng.random(n)
    added_epoch = (lower + u * (upper - lower)).astype(np.int64)

    title_idx = title_pool.sample_indices(n, rng)
    event_id = sequential_ids("wl_", event_id_start, n, 9)

    return pa.table(
        {
            "watchlist_event_id": pa.array(event_id),
            "subscriber_id": pa.array(subscriber_result.subscriber_ids[sub_idx]),
            "title_id": pa.array(title_pool.ids[title_idx]),
            "added_at": pa.array(added_epoch.astype("datetime64[s]")),
        }
    )


def generate_watchlist_events(
    run: RunConfig,
    subscriber_result: SubscriberGenerationResult,
    title_result: TitleGenerationResult,
    output_dir: Path,
) -> WatchlistGenerationSummary:
    scale = run.scale

    subscriber_pool = WeightedPool.build(
        subscriber_result.subscriber_ids,
        child_rng(run.seed, "watchlist.weights.subscriber"),
        skew=0.6,
    )
    title_pool = WeightedPool.build(
        title_result.title_ids, child_rng(run.seed, "watchlist.weights.title"), skew=1.0
    )

    total_rows = scale.watchlist_event_count
    batch_size = scale.watchlist_batch_size
    num_batches = math.ceil(total_rows / batch_size)

    event_id_cursor = 0
    rows_written = 0

    for i in range(num_batches):
        n = min(batch_size, total_rows - i * batch_size)
        if n <= 0:
            break
        rng = child_rng(run.seed, f"watchlist.batch.{i}")
        table_i = _build_batch(
            rng, n, subscriber_result, subscriber_pool, title_pool, event_id_cursor
        )
        event_id_cursor += n
        write_batch(table_i, output_dir, i)
        rows_written += table_i.num_rows

    return {"rows_written": rows_written, "num_batches": num_batches}
