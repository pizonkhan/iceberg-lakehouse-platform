"""Deterministic RNG stream derivation.

Every random draw anywhere in this package must come from a generator built
here, never from np.random.default_rng() called bare or from the global
numpy random state. A label-keyed stream (rather than a sequential spawn
index) means "playback.batch.7" produces identical output regardless of
what else has run before it in a given invocation, so re-running only part
of the pipeline during development never perturbs unrelated streams.
"""

from __future__ import annotations

import zlib

import numpy as np


def child_rng(seed: int, label: str) -> np.random.Generator:
    """Build an independent, deterministic generator for one named purpose.

    `seed` is the single project-wide SEED from generation.config. `label`
    identifies the stream, e.g. "subscribers.core" or "playback.batch.0007".
    Two calls with the same (seed, label) always produce the same stream;
    two different labels never correlate.
    """
    label_hash = zlib.crc32(label.encode("utf-8"))
    seed_sequence = np.random.SeedSequence([seed, label_hash])
    return np.random.default_rng(seed_sequence)
