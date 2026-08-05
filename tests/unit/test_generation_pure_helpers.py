"""Tests for the handful of standalone pure functions embedded in the
pathology-injection generators (subscribers.py, titles.py).

Most of playback.py, billing.py, subscribers.py, titles.py, signup_funnel.py
and watchlist.py is tightly coupled to bulk numpy array construction and a
RunConfig/SubscriberGenerationResult threaded in from generate.py: not
practically unit-testable in isolation at this project's stage (see the
test report for the full breakdown). _profile_upper (subscribers.py) and
_language_weights (titles.py) are the two genuine exceptions: real pure
functions with no numpy-array-construction coupling, isolatable and worth
testing directly.
"""

from __future__ import annotations

import numpy as np

from generation.subscribers import _profile_upper
from generation.titles import LANGUAGES, _language_weights


def test_profile_upper_at_u_zero_is_zero() -> None:
    """u=0 means "drawn right at platform launch": zero elapsed span."""
    assert _profile_upper(seconds_span=10_000.0, u=0.0) == 0.0


def test_profile_upper_at_u_one_reaches_the_full_span() -> None:
    """u=1 means "drawn right at now": the full elapsed span."""
    assert _profile_upper(seconds_span=10_000.0, u=1.0) == 10_000.0


def test_profile_upper_is_monotonically_increasing_in_u() -> None:
    span = 50_000.0
    values = [_profile_upper(span, u) for u in (0.0, 0.25, 0.5, 0.75, 1.0)]
    assert values == sorted(values)
    assert len(set(values)) == len(values)  # strictly increasing, no ties


def test_profile_upper_front_loads_growth_toward_now() -> None:
    """The documented behavior: growth_exponent < 1 skews signups toward
    "now" relative to a uniform (linear) spread, i.e. for u in (0, 1) the
    mapped time is later than a plain linear u * seconds_span would give."""
    span = 100_000.0
    for u in (0.1, 0.3, 0.5, 0.7, 0.9):
        assert _profile_upper(span, u) > span * u


def test_profile_upper_growth_exponent_of_one_is_linear() -> None:
    span = 40_000.0
    assert _profile_upper(span, 0.37, growth_exponent=1.0) == span * 0.37


def test_profile_upper_is_pure_and_deterministic() -> None:
    assert _profile_upper(12_345.0, 0.42) == _profile_upper(12_345.0, 0.42)


def test_language_weights_length_matches_languages_domain() -> None:
    weights = _language_weights()
    assert len(weights) == len(LANGUAGES)


def test_language_weights_sum_to_one() -> None:
    weights = _language_weights()
    assert np.isclose(weights.sum(), 1.0)


def test_language_weights_are_all_positive() -> None:
    weights = _language_weights()
    assert (weights > 0).all()


def test_language_weights_english_dominant() -> None:
    """The module docstring promises an "English-dominant catalog with a
    long tail"; English (index 0 in LANGUAGES) must carry the largest
    weight, not merely a positive one."""
    weights = _language_weights()
    assert LANGUAGES[0] == "en"
    assert weights[0] == weights.max()


def test_language_weights_is_deterministic() -> None:
    np.testing.assert_array_equal(_language_weights(), _language_weights())
