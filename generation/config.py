"""Configuration for the synthetic source-system data generator.

Every random generator used anywhere under generation/ must derive from the
single SEED constant below, via generation.rng.child_rng. Nothing else in
this package hardcodes a seed. See .notes/decisions.md for the scale and
pathology numbers, and why they are set where they are.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, ConfigDict

# The one seed. Every RNG stream in this package is derived from this value
# plus a stable string label (generation.rng.child_rng), never from a bare
# unseeded generator and never reseeded inline in a module.
SEED: int = 20260804

# Simulated wall-clock moment the source systems are extracted as of. Chosen
# to line up with the day this generator was built, so "future timestamp"
# malformed rows and "now" boundaries read coherently together.
GENERATION_NOW: datetime = datetime(2026, 8, 4, 0, 0, 0)

# Lower bound for all generated event timestamps, matching dim_date's floor
# in .notes/modeling.md. This is also the earliest instant any playback or
# watchlist session timestamp can take (generation/playback.py's batch 0
# and generation/watchlist.py both sample starting exactly at this value),
# which is why the title catalog seed window below is anchored to end
# before it rather than sharing it as a start point.
PLATFORM_LAUNCH: datetime = datetime(2023, 1, 1, 0, 0, 0)

# How long before PLATFORM_LAUNCH the title catalog was seeded, and how much
# clearance to leave between the end of that seeding window and
# PLATFORM_LAUNCH itself. Modeling.md's dim_title design assumes "titles
# arrive from a controlled catalog feed ahead of playback"; generation/
# titles.py used to draw catalog_add_at from the same [PLATFORM_LAUNCH, now)
# span playback and watchlist draw their own session timestamps from, with
# no correlation between the two, so a title could easily get its first
# playback session before its own catalog_add event (see .notes/failures.md
# for the full incident writeup). Every title's catalog_add_at is instead
# drawn from [PLATFORM_LAUNCH - CATALOG_SEED_LEAD_TIME - CATALOG_SEED_BUFFER,
# PLATFORM_LAUNCH - CATALOG_SEED_BUFFER], strictly before PLATFORM_LAUNCH,
# so it precedes every possible playback or watchlist timestamp by
# construction rather than by chance. The buffer exists so the window's
# latest edge is not flush against PLATFORM_LAUNCH, keeping "before" true
# with margin rather than by a knife-edge float comparison.
CATALOG_SEED_LEAD_TIME: timedelta = timedelta(days=180)
CATALOG_SEED_BUFFER: timedelta = timedelta(days=1)

# Upper bound dim_date can represent. Malformed "future timestamp" rows are
# kept under this so they are still a resolvable (if nonsensical) date, not
# an out-of-range one.
DATE_DIM_UPPER_BOUND: datetime = datetime(2027, 12, 31, 23, 59, 59)

OUTPUT_ROOT: Path = Path(__file__).resolve().parent / "output"
MANIFEST_DIR_NAME: str = "_pathology_manifest"


class ScaleConfig(BaseModel):
    """Row-count targets for one generation run. Two presets: SMALL (fast
    validation) and FULL (the real deliverable). Facts are drawn from a
    weighted sample of the entity pools below, so actual emitted counts
    land close to these but are not forced to hit them exactly (batch
    rounding, pathology insertions/removals shift the total by a small,
    documented amount).
    """

    model_config = ConfigDict(frozen=True)

    subscriber_count: int
    subscriber_event_count_target: int
    title_count: int
    title_event_count_target: int
    plan_count: int
    device_count: int
    playback_event_count: int
    playback_batch_size: int
    billing_event_count: int
    billing_batch_size: int
    watchlist_event_count: int
    watchlist_batch_size: int
    signup_funnel_count: int


class PathologyConfig(BaseModel):
    """Counts and fractions for the deliberately injected data pathologies.
    Each field is referenced from exactly one generator module; see
    .notes/decisions.md for the full map of pathology -> file -> mechanism.
    """

    model_config = ConfigDict(frozen=True)

    # 1. late-arriving dimension members
    late_arriving_subscriber_count: int

    # 2. late-arriving facts against a historical (non-newest) dim version
    midstream_playback_target_count: int
    midstream_billing_target_count: int

    # 5. duplicate business keys
    duplicate_signup_fraction: float
    duplicate_billing_fraction: float

    # 6. out-of-order playback arrival (delayed-pool straggler mechanism)
    out_of_order_fraction: float
    out_of_order_delay_batches_min: int
    out_of_order_delay_batches_max: int

    # 7. null join keys
    null_device_id_fraction: float

    # 8. same-day multiple attribute changes
    same_day_multi_change_subscriber_count: int

    # 9. mid-stream schema drift
    schema_drift_cutoff_fraction: float

    # 10. mixed soft and hard deletes
    soft_delete_subscriber_count: int
    hard_delete_subscriber_count: int

    # 11. malformed rows
    malformed_playback_fraction: float


FULL_SCALE = ScaleConfig(
    subscriber_count=50_000,
    subscriber_event_count_target=200_000,
    title_count=5_000,
    title_event_count_target=15_000,
    plan_count=30,
    device_count=3_000,
    playback_event_count=120_000_000,
    playback_batch_size=5_000_000,
    billing_event_count=1_500_000,
    billing_batch_size=500_000,
    watchlist_event_count=750_000,
    watchlist_batch_size=250_000,
    signup_funnel_count=70_000,
)

# ~1/50th scale, subscriber_count pinned to 1000 per the validation
# instructions, everything else scaled proportionally and rounded.
SMALL_SCALE = ScaleConfig(
    subscriber_count=1_000,
    subscriber_event_count_target=4_000,
    title_count=100,
    title_event_count_target=300,
    plan_count=30,
    device_count=100,
    playback_event_count=2_400_000,
    playback_batch_size=200_000,
    billing_event_count=30_000,
    billing_batch_size=10_000,
    watchlist_event_count=15_000,
    watchlist_batch_size=5_000,
    signup_funnel_count=1_400,
)

FULL_PATHOLOGY = PathologyConfig(
    late_arriving_subscriber_count=200,
    midstream_playback_target_count=300,
    midstream_billing_target_count=100,
    duplicate_signup_fraction=0.01,
    duplicate_billing_fraction=0.01,
    out_of_order_fraction=0.03,
    out_of_order_delay_batches_min=3,
    out_of_order_delay_batches_max=15,
    null_device_id_fraction=0.02,
    same_day_multi_change_subscriber_count=500,
    schema_drift_cutoff_fraction=0.4,
    soft_delete_subscriber_count=150,
    hard_delete_subscriber_count=150,
    malformed_playback_fraction=0.003,
)

# Counts scaled down ~1/20 (not the full 1/50) relative to FULL so small-scale
# pathologies stay comfortably above the minimums the tester needs to find
# ("a few hundred", "a small number") even after proportional shrinkage.
# Fractions are left identical to FULL: they are rates, scale-invariant by
# construction, and testing them at the same rate is the point.
SMALL_PATHOLOGY = PathologyConfig(
    late_arriving_subscriber_count=15,
    midstream_playback_target_count=20,
    midstream_billing_target_count=8,
    duplicate_signup_fraction=0.01,
    duplicate_billing_fraction=0.01,
    out_of_order_fraction=0.03,
    out_of_order_delay_batches_min=2,
    out_of_order_delay_batches_max=6,
    null_device_id_fraction=0.02,
    same_day_multi_change_subscriber_count=25,
    schema_drift_cutoff_fraction=0.4,
    soft_delete_subscriber_count=12,
    hard_delete_subscriber_count=12,
    malformed_playback_fraction=0.003,
)


class RunConfig(BaseModel):
    """Everything one invocation of generate.py needs, bundled so the CLI
    has a single object to pass around instead of threading six arguments
    through every function.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    seed: int = SEED
    now: datetime = GENERATION_NOW
    platform_launch: datetime = PLATFORM_LAUNCH
    date_dim_upper_bound: datetime = DATE_DIM_UPPER_BOUND
    catalog_seed_lead_time: timedelta = CATALOG_SEED_LEAD_TIME
    catalog_seed_buffer: timedelta = CATALOG_SEED_BUFFER
    output_root: Path = OUTPUT_ROOT
    scale: ScaleConfig
    pathology: PathologyConfig

    @classmethod
    def for_scale(cls, name: str) -> RunConfig:
        if name == "small":
            return cls(scale=SMALL_SCALE, pathology=SMALL_PATHOLOGY)
        if name == "full":
            return cls(scale=FULL_SCALE, pathology=FULL_PATHOLOGY)
        raise ValueError(f"unknown scale preset: {name!r}, expected 'small' or 'full'")
