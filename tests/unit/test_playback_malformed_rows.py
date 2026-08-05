"""Tests for generation/playback.py's _apply_malformed_rows: pathology 11
(malformed rows: negative duration, ended-before-started, future timestamp).

This is the one piece of playback.py's pathology logic that is genuinely
separable from the surrounding vectorized batch-generation flow: it takes
a pre-built arrays dict, a fraction, and a few epoch bounds, and mutates
the arrays in place while returning a manifest describing what it did. No
subscriber/title pooling or Parquet writing is required to exercise it.
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import numpy.typing as npt
import pytest

from generation.config import RunConfig
from generation.playback import (
    MALFORMED_ENDED_BEFORE_STARTED,
    MALFORMED_FUTURE_TIMESTAMP,
    MALFORMED_NEGATIVE_DURATION,
    _apply_malformed_rows,
)
from generation.rng import child_rng
from generation.timeutil import to_epoch_s

BASE_STARTED_EPOCH_S = 1_700_000_000


@pytest.fixture
def now_epoch_s(small_run_config: RunConfig) -> int:
    """The project's real GENERATION_NOW (via the small-scale RunConfig
    fixture), not a value invented for this test file, so "future
    timestamp" bounds line up with what generate_playback_events itself
    uses."""
    return to_epoch_s(small_run_config.now)


@pytest.fixture
def date_dim_upper_epoch_s(small_run_config: RunConfig) -> int:
    """The project's real DATE_DIM_UPPER_BOUND, the ceiling malformed
    future-timestamp rows must stay under."""
    return to_epoch_s(small_run_config.date_dim_upper_bound)


@pytest.fixture
def make_arrays() -> Callable[[int], dict[str, npt.NDArray[np.generic]]]:
    """Build a minimal, well-formed arrays dict shaped like the subset of
    generate_playback_events' per-batch arrays that _apply_malformed_rows
    actually reads and mutates."""

    def _build(n: int) -> dict[str, npt.NDArray[np.generic]]:
        started = BASE_STARTED_EPOCH_S + np.arange(n, dtype=np.int64) * 100
        duration = np.full(n, 3_600, dtype=np.int64)
        return {
            "playback_session_id": np.array([f"pb_{i:012d}" for i in range(n)]),
            "subscriber_id": np.array([f"sub_{i % 5:06d}" for i in range(n)]),
            "session_started_at": started,
            "session_ended_at": started + duration,
            "watch_duration_seconds": duration,
        }

    return _build


def _row_index(arrays: dict[str, npt.NDArray[np.generic]], session_id: str) -> int:
    matches = np.flatnonzero(arrays["playback_session_id"] == session_id)
    assert len(matches) == 1
    return int(matches[0])


def test_zero_fraction_malforms_nothing(
    make_arrays: Callable[[int], dict[str, npt.NDArray[np.generic]]],
    now_epoch_s: int,
    date_dim_upper_epoch_s: int,
) -> None:
    arrays = make_arrays(200)
    original_duration = arrays["watch_duration_seconds"].copy()
    rng = child_rng(1, "test.malformed.zero")

    manifest = _apply_malformed_rows(
        rng, arrays, 0.0, now_epoch_s, date_dim_upper_epoch_s, hard_delete_ids=set()
    )

    assert manifest == []
    np.testing.assert_array_equal(arrays["watch_duration_seconds"], original_duration)


def test_full_fraction_produces_one_manifest_entry_per_row(
    make_arrays: Callable[[int], dict[str, npt.NDArray[np.generic]]],
    now_epoch_s: int,
    date_dim_upper_epoch_s: int,
) -> None:
    n = 500
    arrays = make_arrays(n)
    rng = child_rng(1, "test.malformed.full")

    manifest = _apply_malformed_rows(
        rng, arrays, 1.0, now_epoch_s, date_dim_upper_epoch_s, hard_delete_ids=set()
    )

    assert len(manifest) == n
    ids_seen = {row["playback_session_id"] for row in manifest}
    assert len(ids_seen) == n  # every row malformed exactly once


def test_every_malformation_type_matches_its_actual_row_mutation(
    make_arrays: Callable[[int], dict[str, npt.NDArray[np.generic]]],
    now_epoch_s: int,
    date_dim_upper_epoch_s: int,
) -> None:
    """The manifest's malformation_type must actually describe what
    happened to that row: this is the core "decide whether/how a row is
    malformed" logic pathology 11 depends on."""
    n = 500
    arrays = make_arrays(n)
    rng = child_rng(1, "test.malformed.consistency")

    manifest = _apply_malformed_rows(
        rng, arrays, 1.0, now_epoch_s, date_dim_upper_epoch_s, hard_delete_ids=set()
    )

    seen_types: set[str] = set()
    for row in manifest:
        idx = _row_index(arrays, row["playback_session_id"])
        malformation = row["malformation_type"]
        seen_types.add(malformation)

        if malformation == MALFORMED_NEGATIVE_DURATION:
            assert arrays["watch_duration_seconds"][idx] < 0
        elif malformation == MALFORMED_ENDED_BEFORE_STARTED:
            assert arrays["session_ended_at"][idx] < arrays["session_started_at"][idx]
        elif malformation == MALFORMED_FUTURE_TIMESTAMP:
            assert arrays["session_started_at"][idx] > now_epoch_s
            # capped so the row stays inside a resolvable dim_date range
            assert arrays["session_started_at"][idx] <= date_dim_upper_epoch_s - 7_200
        else:
            pytest.fail(f"unexpected malformation_type: {malformation!r}")

    # with 500 rows and 3 kinds chosen uniformly at random, expect to see
    # all three kinds represented (astronomically unlikely not to)
    assert seen_types == {
        MALFORMED_NEGATIVE_DURATION,
        MALFORMED_ENDED_BEFORE_STARTED,
        MALFORMED_FUTURE_TIMESTAMP,
    }


def test_hard_deleted_subscribers_never_get_a_future_timestamp_malformation(
    make_arrays: Callable[[int], dict[str, npt.NDArray[np.generic]]],
    now_epoch_s: int,
    date_dim_upper_epoch_s: int,
) -> None:
    """Pathology 10 (hard delete) promises a hard-deleted subscriber has no
    rows past their hard_delete_at. A future-timestamp malformation would
    silently plant one, so the function must downgrade that case to a
    negative-duration malformation instead (generation/playback.py's
    _apply_malformed_rows, the `k == 2 and ... in hard_delete_ids` guard)."""
    n = 500
    arrays = make_arrays(n)
    all_subscriber_ids = set(arrays["subscriber_id"].tolist())
    rng = child_rng(1, "test.malformed.hard_delete")

    manifest = _apply_malformed_rows(
        rng,
        arrays,
        1.0,
        now_epoch_s,
        date_dim_upper_epoch_s,
        hard_delete_ids=all_subscriber_ids,
    )

    malformation_types = {row["malformation_type"] for row in manifest}
    assert MALFORMED_FUTURE_TIMESTAMP not in malformation_types
    assert malformation_types <= {MALFORMED_NEGATIVE_DURATION, MALFORMED_ENDED_BEFORE_STARTED}


def test_malformed_row_selection_is_deterministic_given_the_same_seed(
    make_arrays: Callable[[int], dict[str, npt.NDArray[np.generic]]],
    now_epoch_s: int,
    date_dim_upper_epoch_s: int,
) -> None:
    manifest_a = _apply_malformed_rows(
        child_rng(1, "test.malformed.determinism"),
        make_arrays(300),
        0.5,
        now_epoch_s,
        date_dim_upper_epoch_s,
        hard_delete_ids=set(),
    )
    manifest_b = _apply_malformed_rows(
        child_rng(1, "test.malformed.determinism"),
        make_arrays(300),
        0.5,
        now_epoch_s,
        date_dim_upper_epoch_s,
        hard_delete_ids=set(),
    )
    assert manifest_a == manifest_b
