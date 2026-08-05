"""Tests for generation/ids.py: id-pool formatting and weighted sampling.

Covers build_id_pool / sequential_ids (format, uniqueness, offset
continuation) and WeightedPool (cumulative-weight construction, uniform vs
skewed distributions, and the floating-point boundary clamp documented in
sample_indices).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import numpy.typing as npt
import pytest

from generation.ids import WeightedPool, build_id_pool, sequential_ids


def test_build_id_pool_formats_zero_padded_sequential_ids() -> None:
    """build_id_pool("sub_", 3, 6) documented example."""
    ids = build_id_pool("sub_", 3, 6)
    assert list(ids) == ["sub_000000", "sub_000001", "sub_000002"]


def test_build_id_pool_width_pads_to_requested_digits() -> None:
    ids = build_id_pool("tt_", 2, 5)
    assert list(ids) == ["tt_00000", "tt_00001"]


def test_build_id_pool_ids_are_unique_within_a_batch() -> None:
    ids = build_id_pool("dev_", 5_000, 5)
    assert len(set(ids)) == 5_000


def test_build_id_pool_is_deterministic() -> None:
    first = build_id_pool("plan_", 100, 2)
    second = build_id_pool("plan_", 100, 2)
    np.testing.assert_array_equal(first, second)


def test_sequential_ids_from_zero_matches_build_id_pool() -> None:
    """sequential_ids with start=0 is exactly build_id_pool."""
    a = build_id_pool("pb_", 10, 12)
    b = sequential_ids("pb_", 0, 10, 12)
    np.testing.assert_array_equal(a, b)


def test_sequential_ids_continues_a_running_counter() -> None:
    """A later batch's ids pick up immediately after an earlier batch's,
    with no gap and no overlap, as generate_playback_events relies on via
    session_id_cursor."""
    first_batch = sequential_ids("pb_", 0, 5, 12)
    second_batch = sequential_ids("pb_", 5, 5, 12)

    assert list(first_batch)[-1] == "pb_000000000004"
    assert next(iter(second_batch)) == "pb_000000000005"
    assert set(first_batch).isdisjoint(set(second_batch))


def test_sequential_ids_offset_ids_are_unique() -> None:
    ids = sequential_ids("bill_", 1_000, 2_000, 9)
    assert len(set(ids)) == 2_000


class _FixedDraws:
    """Minimal stand-in for np.random.Generator exposing only .random().

    Used to force WeightedPool.sample_indices to see specific draw values
    (including exactly 1.0), which real rng.random() output can produce in
    principle but is not practical to force from a real Generator. This
    exercises the documented boundary clamp directly: "floating point
    rounding can push the last draw one past the end" in
    generation/ids.py's WeightedPool.sample_indices.
    """

    def __init__(self, values: npt.NDArray[np.float64]) -> None:
        self._values = values

    def random(self, n: int) -> npt.NDArray[np.float64]:
        assert n == len(self._values)
        return self._values


def test_weighted_pool_sample_indices_clamps_draw_at_the_top_edge() -> None:
    ids = np.array(["a", "b", "c"])
    pool = WeightedPool.build_uniform(ids)
    # cumulative_weights is [1/3, 2/3, 1.0]; a draw of exactly 1.0 pushes
    # searchsorted(..., side="right") to index 3, one past the last id.
    draws = np.array([1.0, 0.0, 0.5])
    fixed_rng: Any = _FixedDraws(draws)
    indices = pool.sample_indices(3, fixed_rng)
    assert indices.max() <= len(ids) - 1
    assert indices[0] == len(ids) - 1  # clamped, not out of range


def test_weighted_pool_build_uniform_gives_equal_cumulative_steps() -> None:
    ids = np.array(["a", "b", "c", "d"])
    pool = WeightedPool.build_uniform(ids)
    np.testing.assert_allclose(pool.cumulative_weights, [0.25, 0.5, 0.75, 1.0])


def test_weighted_pool_cumulative_weights_end_at_one_and_are_nondecreasing() -> None:
    ids = build_id_pool("tt_", 200, 5)
    rng = np.random.default_rng(42)
    pool = WeightedPool.build(ids, rng, skew=1.1)

    assert pool.cumulative_weights[-1] == pytest.approx(1.0)
    assert np.all(np.diff(pool.cumulative_weights) >= 0)


def test_weighted_pool_zero_skew_is_equivalent_to_uniform() -> None:
    """skew=0.0 makes every rank's weight 1/rank**0 == 1, i.e. uniform,
    regardless of which id the random permutation assigned to which rank."""
    ids = build_id_pool("sub_", 50, 6)
    rng = np.random.default_rng(7)
    pool = WeightedPool.build(ids, rng, skew=0.0)
    expected = np.arange(1, 51) / 50
    np.testing.assert_allclose(pool.cumulative_weights, expected)


def test_weighted_pool_positive_skew_is_not_uniform() -> None:
    """A Zipf-like skew (the project's default 1.1) must actually produce an
    unequal distribution, not silently degrade to uniform."""
    ids = build_id_pool("sub_", 200, 6)
    rng = np.random.default_rng(7)
    pool = WeightedPool.build(ids, rng, skew=1.1)
    weights = np.diff(np.concatenate([[0.0], pool.cumulative_weights]))
    assert weights.max() > weights.min() * 2


def test_weighted_pool_build_is_deterministic_given_the_same_rng_state() -> None:
    ids = build_id_pool("tt_", 30, 5)
    pool_a = WeightedPool.build(ids, np.random.default_rng(99), skew=1.2)
    pool_b = WeightedPool.build(ids, np.random.default_rng(99), skew=1.2)
    np.testing.assert_array_equal(pool_a.cumulative_weights, pool_b.cumulative_weights)


def test_weighted_pool_sample_ids_returns_ids_from_the_pool() -> None:
    ids = np.array(["x", "y", "z"])
    pool = WeightedPool.build_uniform(ids)
    rng = np.random.default_rng(3)
    sampled = pool.sample_ids(1_000, rng)
    assert set(sampled).issubset({"x", "y", "z"})
    assert len(sampled) == 1_000
