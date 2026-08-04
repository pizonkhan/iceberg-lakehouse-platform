"""Shared result types passed between generator modules.

generate.py runs subscribers and titles first and threads their results
into the fact generators, so facts can sample subscriber_id/title_id
values that respect each entity's valid time window (signup through
hard-delete or "now") and can locate the specific rows pathology 2 needs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypedDict

import numpy as np
import numpy.typing as npt


@dataclass(frozen=True)
class MidstreamCandidate:
    """One (subscriber, historical-version-gap) pair eligible for
    pathology 2: a fact timestamped inside this gap must resolve to the
    OLDER version bounded by [gap_start, gap_end), not the newest version
    of the subscriber, since newer_version_start > gap_end exists.
    """

    subscriber_id: str
    gap_start: np.datetime64
    gap_end: np.datetime64
    newer_version_start: np.datetime64


@dataclass
class SubscriberGenerationResult:
    # pool order is authoritative: index i in every array below describes
    # the same subscriber as subscriber_ids[i].
    subscriber_ids: npt.NDArray[np.str_]
    signup_epoch_s: npt.NDArray[np.int64]
    upper_bound_epoch_s: npt.NDArray[np.int64]  # "now" or hard_delete_at
    is_hard_deleted: npt.NDArray[np.bool_]

    late_arrival_ids: set[str]
    hard_delete_ids: set[str]
    soft_delete_ids: set[str]
    same_day_ids: set[str]

    midstream_candidates: list[MidstreamCandidate] = field(default_factory=list)
    total_events_emitted: int = 0

    # rows actually written, split so late-arrival subscribers' own change
    # events can be forced to the physically last file (pathology 1)
    hard_delete_at_by_id: dict[str, np.datetime64] = field(default_factory=dict)


@dataclass
class TitleGenerationResult:
    title_ids: npt.NDArray[np.str_]
    total_events_emitted: int = 0


@dataclass
class ReferenceData:
    plan_ids: npt.NDArray[np.str_]
    device_ids: npt.NDArray[np.str_]


class PlaybackGenerationSummary(TypedDict):
    rows_written: int
    num_batches: int
    cutoff_batch: int
    cutoff_boundary_at: str
    straggler_rows: int
    malformed_manifest: list[dict[str, str]]
    midstream_manifest: list[dict[str, str]]


class BillingGenerationSummary(TypedDict):
    rows_written: int
    num_batches: int
    duplicate_count: int
    duplicate_ids: list[dict[str, str]]
    midstream_manifest: list[dict[str, str]]


class WatchlistGenerationSummary(TypedDict):
    rows_written: int
    num_batches: int


class SignupFunnelGenerationSummary(TypedDict):
    rows_written: int
    duplicate_count: int
    duplicate_ids: list[dict[str, str]]
