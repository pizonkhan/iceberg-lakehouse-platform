"""Title metadata change-event stream (source-shaped catalog feed).

Same change-event-stream shape as subscribers.py: an initial catalog-add
event per title plus follow-on metadata-revision events (rating corrections,
runtime fixes, re-releases). No pathologies are required against titles
specifically in the work package; dim_title has no is_inferred handling
per .notes/modeling.md (an unseen title takes the unknown member and is a
quality-gate failure, not something this generator needs to construct).
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import numpy.typing as npt
import pyarrow as pa

from generation.config import RunConfig
from generation.ids import build_id_pool
from generation.rng import child_rng
from generation.schemas import TitleGenerationResult

CONTENT_TYPES = ["movie", "series"]
CONTENT_TYPE_WEIGHTS = [0.62, 0.38]
MATURITY_RATINGS = ["G", "PG", "PG-13", "R", "NC-17", "TV-Y", "TV-PG", "TV-14", "TV-MA"]
LANGUAGES = ["en", "es", "fr", "de", "ja", "ko", "pt", "hi", "it", "sv"]

TITLE_ADJECTIVES = [
    "Silent",
    "Broken",
    "Last",
    "Hidden",
    "Endless",
    "Crimson",
    "Distant",
    "Forgotten",
    "Golden",
    "Midnight",
    "Wild",
    "Quiet",
    "Falling",
    "Northern",
    "Ancient",
    "Electric",
]
TITLE_NOUNS = [
    "River",
    "Kingdom",
    "Signal",
    "Harbor",
    "Circuit",
    "Garden",
    "Horizon",
    "Mirror",
    "Station",
    "Empire",
    "Orchard",
    "Frontier",
    "Archive",
    "Descent",
    "Chorus",
    "Ledger",
]


def generate_titles(run: RunConfig) -> tuple[TitleGenerationResult, list[dict[str, object]]]:
    scale = run.scale
    n = scale.title_count
    rng = child_rng(run.seed, "titles.core")

    title_ids = build_id_pool("tt_", n, 5)

    # Catalog seeding happens strictly before PLATFORM_LAUNCH, not spread
    # across the same [platform_launch, now) span playback.py and
    # watchlist.py draw their own session timestamps from. Those two both
    # sample starting exactly at platform_launch (see config.py's
    # CATALOG_SEED_LEAD_TIME comment), so anchoring every title's first
    # catalog_add_at to a window that ends before platform_launch
    # guarantees it precedes any playback or watchlist event for that
    # title by construction, not by chance. See .notes/failures.md.
    catalog_window_end = run.platform_launch - run.catalog_seed_buffer
    catalog_window_start = catalog_window_end - run.catalog_seed_lead_time
    catalog_span_s = (catalog_window_end - catalog_window_start).total_seconds()

    events: list[dict[str, object]] = []

    for i in range(n):
        title_id = title_ids[i]
        u = rng.random()
        catalog_add_at = catalog_window_start + timedelta(seconds=catalog_span_s * u)
        catalog_add_at = min(catalog_add_at, catalog_window_end)

        content_type = str(rng.choice(CONTENT_TYPES, p=CONTENT_TYPE_WEIGHTS))
        title_name = (
            f"{TITLE_ADJECTIVES[rng.integers(len(TITLE_ADJECTIVES))]} "
            f"{TITLE_NOUNS[rng.integers(len(TITLE_NOUNS))]}"
        )
        release_year = int(rng.integers(1995, run.now.year + 1))
        runtime_minutes = None if content_type == "series" else int(rng.integers(75, 180))
        maturity_rating = str(rng.choice(MATURITY_RATINGS))
        original_language = str(rng.choice(LANGUAGES, p=_language_weights()))
        is_original = bool(rng.random() < 0.15)

        events.append(
            {
                "title_id": title_id,
                "change_event_id": f"ttev_{len(events):08d}",
                "change_timestamp": catalog_add_at,
                "change_type": "catalog_add",
                "title_name": title_name,
                "content_type": content_type,
                "release_year": release_year,
                "runtime_minutes": runtime_minutes,
                "maturity_rating": maturity_rating,
                "original_language": original_language,
                "is_original": is_original,
            }
        )

        follow_on_count = int(rng.poisson(2))
        cursor_ts = catalog_add_at
        for _ in range(follow_on_count):
            remaining = (run.now - cursor_ts).total_seconds()
            if remaining <= 60:
                break
            cursor_ts = cursor_ts + timedelta(seconds=remaining * rng.uniform(0.1, 0.7))
            if rng.random() < 0.4:
                maturity_rating = str(rng.choice(MATURITY_RATINGS))
            if rng.random() < 0.2 and runtime_minutes is not None:
                runtime_minutes = max(20, runtime_minutes + int(rng.integers(-10, 10)))
            events.append(
                {
                    "title_id": title_id,
                    "change_event_id": f"ttev_{len(events):08d}",
                    "change_timestamp": cursor_ts,
                    "change_type": "metadata_update",
                    "title_name": title_name,
                    "content_type": content_type,
                    "release_year": release_year,
                    "runtime_minutes": runtime_minutes,
                    "maturity_rating": maturity_rating,
                    "original_language": original_language,
                    "is_original": is_original,
                }
            )

    result = TitleGenerationResult(title_ids=title_ids, total_events_emitted=len(events))
    return result, events


def _language_weights() -> npt.NDArray[np.float64]:
    # English-dominant catalog with a long tail, typical of a US-launched
    # streaming service's licensed + original content mix.
    weights = np.array([0.45, 0.09, 0.08, 0.07, 0.07, 0.06, 0.06, 0.05, 0.04, 0.03])
    result: npt.NDArray[np.float64] = weights / weights.sum()
    return result


def events_to_table(events: list[dict[str, object]]) -> pa.Table:
    return pa.Table.from_pylist(events)
