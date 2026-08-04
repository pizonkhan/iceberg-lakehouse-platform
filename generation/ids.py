"""Business-key id pools and vectorized id formatting.

The 120M-row playback fact is the one place row-by-row Python string
formatting would actually cost minutes; everything here is numpy vector
ops so a batch of 5M ids formats in well under a second (benchmarked before
the real generator was written, see .notes/decisions.md).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import numpy.typing as npt


def build_id_pool(prefix: str, count: int, width: int) -> npt.NDArray[np.str_]:
    """Sequential zero-padded ids: build_id_pool("sub_", 3, 6) ->
    ["sub_000000", "sub_000001", "sub_000002"]."""
    indices = np.arange(count, dtype=np.int64)
    return np.char.add(prefix, np.char.zfill(indices.astype(str), width))


def sequential_ids(prefix: str, start: int, count: int, width: int) -> npt.NDArray[np.str_]:
    """Like build_id_pool but starting from an arbitrary offset, for
    generating a batch of grain-key ids that continue a running counter
    across previously written batches."""
    indices = np.arange(start, start + count, dtype=np.int64)
    return np.char.add(prefix, np.char.zfill(indices.astype(str), width))


@dataclass(frozen=True)
class WeightedPool:
    """A fixed pool of ids sampled with a Zipf-like skew rather than
    uniformly, because real engagement/popularity distributions are
    heavily skewed (a minority of titles or subscribers generate a
    disproportionate share of events). Rank is assigned to a random
    permutation of pool positions, so popularity never correlates with
    id numbering (id 000000 is not systematically the most popular).
    """

    ids: npt.NDArray[np.str_]
    cumulative_weights: npt.NDArray[np.float64]

    @classmethod
    def build(
        cls, ids: npt.NDArray[np.str_], rng: np.random.Generator, skew: float = 1.1
    ) -> WeightedPool:
        size = len(ids)
        ranks = rng.permutation(size) + 1
        weights = 1.0 / np.power(ranks.astype(np.float64), skew)
        weights = weights / weights.sum()
        return cls(ids=ids, cumulative_weights=np.cumsum(weights))

    @classmethod
    def build_uniform(cls, ids: npt.NDArray[np.str_]) -> WeightedPool:
        size = len(ids)
        weights = np.full(size, 1.0 / size, dtype=np.float64)
        return cls(ids=ids, cumulative_weights=np.cumsum(weights))

    def sample_indices(self, n: int, rng: np.random.Generator) -> npt.NDArray[np.int64]:
        draws = rng.random(n)
        idx = np.searchsorted(self.cumulative_weights, draws, side="right")
        # floating point rounding can push the last draw one past the end
        return np.minimum(idx, len(self.ids) - 1).astype(np.int64)

    def sample_ids(self, n: int, rng: np.random.Generator) -> npt.NDArray[np.str_]:
        return self.ids[self.sample_indices(n, rng)]
