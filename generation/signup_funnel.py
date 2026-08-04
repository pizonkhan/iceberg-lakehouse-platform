"""fct_signup_funnel source: ~70k signup attempts with milestone timestamps.

Not every attempt reaches every milestone; each stage has a chance of
dropping the attempt, matching a real acquisition funnel's decay. Registered
attempts are tied back to an actual subscriber_id and that subscriber's real
signup timestamp (gathered from subscribers.py's output), so the funnel and
the subscriber stream tell a consistent story. Carries pathology 5
(duplicate signup_id under upstream retry).

Only source-observable fields are emitted: funnel_status and the
hours_* lag measures on fct_signup_funnel in .notes/modeling.md are gold-
computed from these timestamps by the load, not something a source system
would emit.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import numpy.typing as npt
import pyarrow as pa

from generation.config import RunConfig
from generation.ids import sequential_ids
from generation.rng import child_rng
from generation.types import SignupFunnelGenerationSummary, SubscriberGenerationResult
from generation.writer import write_batch

REGISTERED_RATE = 0.90
EMAIL_VERIFIED_RATE = 0.80
PAYMENT_METHOD_RATE = 0.75
PLAN_SELECTED_RATE = 0.85
FIRST_STREAM_RATE = 0.70


def generate_signup_funnel(
    run: RunConfig,
    subscriber_result: SubscriberGenerationResult,
    plan_ids: npt.NDArray[np.str_],
    output_dir: Path,
) -> SignupFunnelGenerationSummary:
    scale = run.scale
    pathology = run.pathology
    n = scale.signup_funnel_count
    rng = child_rng(run.seed, "signup_funnel.core")

    n_subscribers = len(subscriber_result.subscriber_ids)
    sub_idx = rng.integers(0, n_subscribers, n)

    registered_mask = rng.random(n) < REGISTERED_RATE
    email_verified_mask = registered_mask & (rng.random(n) < EMAIL_VERIFIED_RATE)
    payment_mask = email_verified_mask & (rng.random(n) < PAYMENT_METHOD_RATE)
    plan_selected_mask = payment_mask & (rng.random(n) < PLAN_SELECTED_RATE)
    first_stream_mask = plan_selected_mask & (rng.random(n) < FIRST_STREAM_RATE)

    registered_epoch = subscriber_result.signup_epoch_s[sub_idx]
    attempt_started_epoch = registered_epoch - rng.integers(300, 7200, n)

    email_verified_epoch = registered_epoch + rng.integers(60, 21600, n)
    payment_epoch = email_verified_epoch + rng.integers(60, 43200, n)
    plan_selected_epoch = payment_epoch + rng.integers(30, 3600, n)
    first_stream_epoch = plan_selected_epoch + rng.integers(30, 86400, n)

    subscriber_id_col = subscriber_result.subscriber_ids[sub_idx].astype(object)
    subscriber_id_col[~registered_mask] = None
    registered_at_col = np.where(registered_mask, registered_epoch, -1)
    email_verified_at_col = np.where(email_verified_mask, email_verified_epoch, -1)
    payment_at_col = np.where(payment_mask, payment_epoch, -1)
    plan_selected_at_col = np.where(plan_selected_mask, plan_selected_epoch, -1)
    first_stream_at_col = np.where(first_stream_mask, first_stream_epoch, -1)

    plan_idx = rng.integers(0, len(plan_ids), n)
    plan_id_selected_col = plan_ids[plan_idx].astype(object)
    plan_id_selected_col[~plan_selected_mask] = None

    signup_id = sequential_ids("sig_", 0, n, 8)

    def epoch_col_to_arrow(col: npt.NDArray[np.int64]) -> pa.Array:
        arr: npt.NDArray[np.object_] = col.astype(object)
        arr[col < 0] = None
        return pa.array(
            [None if v is None else np.datetime64(int(v), "s") for v in arr], type=pa.timestamp("s")
        )

    table = pa.table(
        {
            "signup_id": pa.array(signup_id),
            "subscriber_id": pa.array(subscriber_id_col, type=pa.string()),
            "attempt_started_at": pa.array(attempt_started_epoch.astype("datetime64[s]")),
            "registered_at": epoch_col_to_arrow(registered_at_col),
            "email_verified_at": epoch_col_to_arrow(email_verified_at_col),
            "payment_method_added_at": epoch_col_to_arrow(payment_at_col),
            "plan_id_selected": pa.array(plan_id_selected_col, type=pa.string()),
            "plan_selected_at": epoch_col_to_arrow(plan_selected_at_col),
            "first_stream_at": epoch_col_to_arrow(first_stream_at_col),
        }
    )

    # pathology 5: duplicate a small fraction of attempts under the
    # identical signup_id, simulating an upstream retry.
    dup_mask = rng.random(n) < pathology.duplicate_signup_fraction
    duplicate_ids: list[dict[str, str]] = []
    if dup_mask.any():
        dup_table = table.filter(pa.array(dup_mask))
        table = pa.concat_tables([table, dup_table])
        for signup_id_val in dup_table.column("signup_id").to_pylist():
            duplicate_ids.append({"signup_id": signup_id_val})

    write_batch(table, output_dir, 0)

    return {
        "rows_written": table.num_rows,
        "duplicate_count": len(duplicate_ids),
        "duplicate_ids": duplicate_ids,
    }
