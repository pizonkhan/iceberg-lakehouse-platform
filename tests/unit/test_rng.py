"""Tests for generation/rng.py: the deterministic per-label RNG stream.

The project's documented reproducibility guarantee (generation/rng.py's
module docstring, and CLAUDE.md's "every random generator explicitly
seeded") rests entirely on child_rng(seed, label): the same (seed, label)
pair must always produce the same draws, and two different labels drawn
from the same seed must never correlate, regardless of what else has run
before them. These tests verify both halves of that guarantee directly.
"""

from __future__ import annotations

import zlib

import numpy as np

from generation.rng import child_rng


def test_same_seed_and_label_produce_identical_streams() -> None:
    """Two independent calls with the same (seed, label) draw identical values.

    This is the property the whole pipeline's reproducibility rests on: a
    developer re-running only one batch during development must get back
    exactly the rows that batch produced in a full run.
    """
    rng_a = child_rng(20260804, "playback.batch.0007")
    rng_b = child_rng(20260804, "playback.batch.0007")

    np.testing.assert_array_equal(rng_a.random(50), rng_b.random(50))


def test_different_labels_produce_independent_streams() -> None:
    """Two different labels under the same seed do not draw the same sequence.

    "subscribers.core" and "playback.weights.subscriber" must not
    accidentally correlate just because they share the project seed.
    """
    rng_a = child_rng(20260804, "subscribers.core")
    rng_b = child_rng(20260804, "playback.weights.subscriber")

    draws_a = rng_a.random(50)
    draws_b = rng_b.random(50)
    assert not np.array_equal(draws_a, draws_b)


def test_different_seeds_same_label_produce_independent_streams() -> None:
    """Changing the seed changes the stream even for an identical label."""
    rng_a = child_rng(1, "titles.core")
    rng_b = child_rng(2, "titles.core")

    draws_a = rng_a.random(50)
    draws_b = rng_b.random(50)
    assert not np.array_equal(draws_a, draws_b)


def test_label_is_order_independent_across_interleaved_draws() -> None:
    """A stream's output does not depend on what else has been drawn first.

    generate.py calls child_rng for many labels across one process
    invocation. If building rng_b's SeedSequence were somehow influenced by
    prior calls (e.g. via hidden global state), interleaving draws from two
    streams would perturb one relative to drawing it in isolation. It must
    not: label-keyed derivation, not a sequential spawn index, is exactly
    what the module docstring promises.
    """
    isolated_a = child_rng(20260804, "billing.batch.0").random(10)
    isolated_b = child_rng(20260804, "watchlist.batch.0").random(10)

    # now draw the same two streams, but interleaved with unrelated calls
    # and in the opposite order
    child_rng(20260804, "some.other.label").random(5)
    interleaved_b = child_rng(20260804, "watchlist.batch.0").random(10)
    child_rng(20260804, "yet.another.label").random(5)
    interleaved_a = child_rng(20260804, "billing.batch.0").random(10)

    np.testing.assert_array_equal(isolated_a, interleaved_a)
    np.testing.assert_array_equal(isolated_b, interleaved_b)


def test_returns_a_real_numpy_generator() -> None:
    """child_rng returns a usable np.random.Generator, not merely something
    that behaves like one."""
    rng = child_rng(20260804, "smoke.test")
    assert isinstance(rng, np.random.Generator)


def test_label_hash_collisions_would_be_a_real_risk_but_common_labels_differ() -> None:
    """Sanity check that the crc32 label hash actually separates a batch of
    realistic label strings used across the generator package (no two
    distinct labels collide onto the same 32-bit hash for this project's
    actual label vocabulary).
    """
    labels = [
        "subscribers.core",
        "subscribers.pathology",
        "titles.core",
        "playback.weights.subscriber",
        "playback.weights.title",
        "playback.pathology.midstream",
        "billing.weights.subscriber",
        "billing.pathology.midstream",
        "watchlist.weights.subscriber",
        "watchlist.weights.title",
        "signup_funnel.core",
        *[f"playback.batch.{i}" for i in range(20)],
        *[f"billing.batch.{i}" for i in range(20)],
    ]
    hashes = {zlib.crc32(label.encode("utf-8")) for label in labels}
    assert len(hashes) == len(labels)
