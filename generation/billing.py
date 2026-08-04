"""fct_billing_transactions source: ~1.5M billing ledger events.

Two of the eleven pathologies live here:
- 2 (partial): the billing share of late-arriving facts against a
  historical dim version (see playback.py for the playback share).
- 5 (partial): duplicate billing_transaction_id, simulating an upstream
  retry that re-emitted the same charge twice.
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pyarrow as pa

from generation.config import RunConfig
from generation.ids import WeightedPool, sequential_ids
from generation.rng import child_rng
from generation.timeutil import from_epoch_s, to_epoch_s
from generation.schemas import BillingGenerationSummary, SubscriberGenerationResult
from generation.writer import write_batch

PAYMENT_TYPES = np.array(["card", "paypal", "gift_card", "carrier_billing"])
PAYMENT_TYPE_WEIGHTS = np.array([0.62, 0.22, 0.09, 0.07])

TRANSACTION_TYPES = np.array(["charge", "refund", "credit", "proration"])
TRANSACTION_TYPE_WEIGHTS = np.array([0.75, 0.10, 0.10, 0.05])


def _build_batch(
    rng: np.random.Generator,
    n: int,
    subscriber_result: SubscriberGenerationResult,
    subscriber_pool: WeightedPool,
    plan_ids: npt.NDArray[np.str_],
    transaction_id_start: int,
) -> pa.Table:
    sub_idx = subscriber_pool.sample_indices(n, rng)
    lower = subscriber_result.signup_epoch_s[sub_idx]
    upper = subscriber_result.upper_bound_epoch_s[sub_idx]
    upper = np.maximum(upper, lower + 1)
    u = rng.random(n)
    posted_epoch = (lower + u * (upper - lower)).astype(np.int64)

    txn_type_idx = rng.choice(len(TRANSACTION_TYPES), size=n, p=TRANSACTION_TYPE_WEIGHTS)
    txn_type = TRANSACTION_TYPES[txn_type_idx]

    base_amount = np.round(np.clip(rng.lognormal(mean=2.4, sigma=0.5, size=n), 2.99, 89.99), 2)
    sign = np.where(np.isin(txn_type, ["refund", "credit"]), -1.0, 1.0)
    # prorations can land either sign, per .notes/modeling.md
    proration_sign_flip = (txn_type == "proration") & (rng.random(n) < 0.5)
    sign = np.where(proration_sign_flip, -1.0, sign)
    amount_usd = np.round(base_amount * sign, 2)
    tax_amount_usd = np.round(np.abs(amount_usd) * 0.0725 * np.sign(amount_usd), 2)

    payment_idx = rng.choice(len(PAYMENT_TYPES), size=n, p=PAYMENT_TYPE_WEIGHTS)
    is_promo = rng.random(n) < 0.12
    is_retry = rng.random(n) < 0.06
    is_autopay = rng.random(n) < 0.7

    plan_idx = rng.integers(0, len(plan_ids), n)

    billing_id = sequential_ids("bill_", transaction_id_start, n, 9)

    return pa.table(
        {
            "billing_transaction_id": pa.array(billing_id),
            "subscriber_id": pa.array(subscriber_result.subscriber_ids[sub_idx]),
            "plan_id": pa.array(plan_ids[plan_idx]),
            "payment_type": pa.array(PAYMENT_TYPES[payment_idx]),
            "is_promo_applied": pa.array(is_promo),
            "is_retry": pa.array(is_retry),
            "is_autopay": pa.array(is_autopay),
            "transaction_type": pa.array(txn_type),
            "transaction_posted_at": pa.array(posted_epoch.astype("datetime64[s]")),
            "amount_usd": pa.array(amount_usd),
            "tax_amount_usd": pa.array(tax_amount_usd),
        }
    )


def generate_billing_events(
    run: RunConfig,
    subscriber_result: SubscriberGenerationResult,
    plan_ids: npt.NDArray[np.str_],
    output_dir: Path,
) -> BillingGenerationSummary:
    scale = run.scale
    pathology = run.pathology

    subscriber_pool = WeightedPool.build(
        subscriber_result.subscriber_ids,
        child_rng(run.seed, "billing.weights.subscriber"),
        skew=0.6,
    )

    total_rows = scale.billing_event_count
    batch_size = scale.billing_batch_size
    num_batches = math.ceil(total_rows / batch_size)

    txn_id_cursor = 0
    rows_written = 0
    duplicate_ids: list[dict[str, str]] = []

    for i in range(num_batches):
        n = min(batch_size, total_rows - i * batch_size)
        if n <= 0:
            break
        rng = child_rng(run.seed, f"billing.batch.{i}")
        table_i = _build_batch(rng, n, subscriber_result, subscriber_pool, plan_ids, txn_id_cursor)
        txn_id_cursor += n

        # pathology 5: duplicate a small fraction of rows under the
        # identical billing_transaction_id, simulating an upstream retry
        # that re-posted the same transaction.
        dup_mask = rng.random(n) < pathology.duplicate_billing_fraction
        if dup_mask.any():
            dup_table = table_i.filter(pa.array(dup_mask))
            table_i = pa.concat_tables([table_i, dup_table])
            for txn_id in dup_table.column("billing_transaction_id").to_pylist():
                duplicate_ids.append({"billing_transaction_id": txn_id})

        write_batch(table_i, output_dir, i)
        rows_written += table_i.num_rows

    midstream_manifest = _write_midstream_billing_targets(
        run, subscriber_result, plan_ids, output_dir, num_batches, txn_id_cursor
    )

    return {
        "rows_written": rows_written,
        "num_batches": num_batches,
        "duplicate_count": len(duplicate_ids),
        "duplicate_ids": duplicate_ids,
        "midstream_manifest": midstream_manifest,
    }


def _write_midstream_billing_targets(
    run: RunConfig,
    subscriber_result: SubscriberGenerationResult,
    plan_ids: npt.NDArray[np.str_],
    output_dir: Path,
    part_index: int,
    txn_id_start: int,
) -> list[dict[str, str]]:
    """Pathology 2, billing half: same construction as
    playback._write_midstream_playback_targets, a disjoint slice of the
    same candidate list so both fact types are exercised.
    """
    pathology = run.pathology
    rng = child_rng(run.seed, "billing.pathology.midstream")

    candidates = subscriber_result.midstream_candidates
    # offset into the candidate list past what playback already consumed,
    # so the two fact types target different subscribers where possible
    offset_start = pathology.midstream_playback_target_count
    pool = candidates[offset_start:] if len(candidates) > offset_start else candidates
    count = min(pathology.midstream_billing_target_count, len(pool))
    if count == 0:
        return []
    chosen_idx = rng.choice(len(pool), size=count, replace=False)

    manifest: list[dict[str, str]] = []
    rows = []
    plan_idx = rng.integers(0, len(plan_ids), size=count)
    for offset, ci in enumerate(chosen_idx):
        candidate = pool[ci]
        gap_start_s = to_epoch_s(candidate.gap_start.astype("datetime64[s]").astype(object))
        gap_end_s = to_epoch_s(candidate.gap_end.astype("datetime64[s]").astype(object))
        span = max(gap_end_s - gap_start_s, 2)
        event_s = gap_start_s + int(rng.uniform(0.1, 0.9) * span)
        billing_id = f"bill_{txn_id_start + offset:09d}"
        amount = round(float(np.clip(rng.lognormal(2.4, 0.5), 2.99, 89.99)), 2)
        rows.append(
            {
                "billing_transaction_id": billing_id,
                "subscriber_id": candidate.subscriber_id,
                "plan_id": str(plan_ids[plan_idx[offset]]),
                "payment_type": str(rng.choice(PAYMENT_TYPES, p=PAYMENT_TYPE_WEIGHTS)),
                "is_promo_applied": bool(rng.random() < 0.12),
                "is_retry": bool(rng.random() < 0.06),
                "is_autopay": bool(rng.random() < 0.7),
                "transaction_type": "charge",
                "transaction_posted_at": event_s,
                "amount_usd": amount,
                "tax_amount_usd": round(amount * 0.0725, 2),
            }
        )
        manifest.append(
            {
                "fact_type": "billing",
                "constructed_id": billing_id,
                "subscriber_id": candidate.subscriber_id,
                "event_timestamp": str(from_epoch_s(event_s)),
                "older_version_effective_from": str(candidate.gap_start),
                "older_version_effective_to": str(candidate.gap_end),
                "newer_version_effective_from": str(candidate.newer_version_start),
            }
        )

    table = pa.table(
        {
            "billing_transaction_id": pa.array([r["billing_transaction_id"] for r in rows]),
            "subscriber_id": pa.array([r["subscriber_id"] for r in rows]),
            "plan_id": pa.array([r["plan_id"] for r in rows]),
            "payment_type": pa.array([r["payment_type"] for r in rows]),
            "is_promo_applied": pa.array([r["is_promo_applied"] for r in rows]),
            "is_retry": pa.array([r["is_retry"] for r in rows]),
            "is_autopay": pa.array([r["is_autopay"] for r in rows]),
            "transaction_type": pa.array([r["transaction_type"] for r in rows]),
            "transaction_posted_at": pa.array(
                np.array([r["transaction_posted_at"] for r in rows], dtype=np.int64).astype(
                    "datetime64[s]"
                )
            ),
            "amount_usd": pa.array([r["amount_usd"] for r in rows]),
            "tax_amount_usd": pa.array([r["tax_amount_usd"] for r in rows]),
        }
    )
    write_batch(table, output_dir, part_index, prefix="part-pathology-midstream")
    return manifest
