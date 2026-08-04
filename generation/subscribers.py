"""Subscriber profile change-event stream (source-shaped, not the gold
Type 6 dimension itself).

Emits one row per profile snapshot at a change moment: the initial signup
event plus follow-on events whenever plan_tier and/or status (and
occasionally the Type 1 attributes) change, matching the shape a real
subscriber-service change feed would emit. A later work package hashes the
tracked attributes to build dim_subscriber's Type 6 history from this
stream.

This module is not the 120M-row hot path (order of 200k rows total), so it
is written as a straightforward per-subscriber loop rather than vectorized
numpy, which makes the pathology bookkeeping (late arrival, same-day
changes, soft/hard delete, mid-history join targets) far easier to reason
about correctly. See generation/playback.py for the vectorized approach
required at fact-table scale.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import numpy as np
import pyarrow as pa

from generation.config import RunConfig
from generation.ids import build_id_pool
from generation.reference import TIERS
from generation.rng import child_rng
from generation.timeutil import to_epoch_s
from generation.types import MidstreamCandidate, SubscriberGenerationResult

STATUS_DOMAIN = ["trial", "active", "paused", "cancelled", "deleted", "past_due"]
# raw source status values that signal a soft delete once emitted; broader
# than dim_subscriber's finalized gold domain (trial/active/paused/churned/
# unknown) on purpose, see .notes/decisions.md.
SOFT_DELETE_STATUSES = ["cancelled", "deleted"]

COUNTRY_CODES = [
    "US",
    "CA",
    "GB",
    "DE",
    "FR",
    "BR",
    "MX",
    "IN",
    "AU",
    "JP",
    "ES",
    "IT",
    "NL",
    "SE",
    "KR",
]
COUNTRY_WEIGHTS = [
    0.38,
    0.07,
    0.09,
    0.07,
    0.06,
    0.05,
    0.04,
    0.05,
    0.04,
    0.03,
    0.03,
    0.03,
    0.02,
    0.02,
    0.02,
]

ACQUISITION_CHANNELS = [
    "organic",
    "paid_search",
    "paid_social",
    "referral",
    "affiliate",
    "email_campaign",
    "app_store",
]

FIRST_NAMES = [
    "James",
    "Mary",
    "Robert",
    "Patricia",
    "John",
    "Jennifer",
    "Michael",
    "Linda",
    "David",
    "Elizabeth",
    "Wei",
    "Fatima",
    "Carlos",
    "Sofia",
    "Yuki",
    "Priya",
    "Ahmed",
    "Ingrid",
    "Noah",
    "Olivia",
    "Liam",
    "Emma",
    "Lucas",
    "Mia",
    "Hiroshi",
    "Anya",
    "Diego",
    "Chidi",
    "Sana",
    "Bjorn",
]
LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Chen",
    "Kim",
    "Silva",
    "Nguyen",
    "Kowalski",
    "Muller",
    "Andersson",
    "Rossi",
    "Dubois",
    "Yamamoto",
    "Okafor",
    "Petrov",
    "Haddad",
    "Lindgren",
]
EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "proton.me"]


def _profile_upper(seconds_span: float, u: float, growth_exponent: float = 0.7) -> float:
    """Front-loads growth toward "now": more signups happened recently than
    at platform launch, a mild power-law skew rather than a uniform spread.
    """
    return float(seconds_span * (u**growth_exponent))


def _random_name(rng: np.random.Generator) -> tuple[str, str]:
    first = FIRST_NAMES[rng.integers(len(FIRST_NAMES))]
    last = LAST_NAMES[rng.integers(len(LAST_NAMES))]
    return str(first), str(last)


def generate_subscribers(
    run: RunConfig,
) -> tuple[SubscriberGenerationResult, list[dict[str, object]]]:
    scale = run.scale
    pathology = run.pathology
    n = scale.subscriber_count

    rng = child_rng(run.seed, "subscribers.core")
    pathology_rng = child_rng(run.seed, "subscribers.pathology")

    subscriber_ids = build_id_pool("sub_", n, 6)

    pathology_pool = pathology_rng.permutation(n)
    cursor = 0

    def take(count: int) -> set[int]:
        nonlocal cursor
        chosen = {int(x) for x in pathology_pool[cursor : cursor + count]}
        cursor += count
        return chosen

    late_arrival_idx = take(min(pathology.late_arriving_subscriber_count, n))
    same_day_idx = take(min(pathology.same_day_multi_change_subscriber_count, n - cursor))
    soft_delete_idx = take(min(pathology.soft_delete_subscriber_count, n - cursor))
    hard_delete_idx = take(min(pathology.hard_delete_subscriber_count, n - cursor))

    total_span_s = (run.now - run.platform_launch).total_seconds()

    events: list[dict[str, object]] = []
    signup_epoch_s = np.zeros(n, dtype=np.int64)
    upper_bound_epoch_s = np.zeros(n, dtype=np.int64)
    is_hard_deleted = np.zeros(n, dtype=np.bool_)
    hard_delete_at_by_id: dict[str, np.datetime64] = {}
    midstream_candidates: list[MidstreamCandidate] = []

    for i in range(n):
        sub_id = subscriber_ids[i]
        u = rng.random()
        signup_at = run.platform_launch + timedelta(seconds=_profile_upper(total_span_s, u))
        # keep at least a day of runway before "now" so there's room for
        # follow-on events
        signup_at = min(signup_at, run.now - timedelta(days=1))

        first, last = _random_name(rng)
        email_local = f"{first.lower()}.{last.lower()}{i}"
        display_name = f"{first} {last}"
        email = f"{email_local}@{EMAIL_DOMAINS[rng.integers(len(EMAIL_DOMAINS))]}"
        country_code = str(rng.choice(COUNTRY_CODES, p=COUNTRY_WEIGHTS))
        acquisition_channel = str(rng.choice(ACQUISITION_CHANNELS))

        plan_tier = str(rng.choice(TIERS, p=[0.35, 0.35, 0.22, 0.08]))
        status = "trial" if rng.random() < 0.55 else "active"

        is_hard_delete = i in hard_delete_idx
        is_soft_delete = i in soft_delete_idx
        is_same_day = i in same_day_idx

        upper_bound = run.now
        hard_delete_at: datetime | None = None
        if is_hard_delete:
            remaining = (run.now - signup_at).total_seconds()
            hard_delete_at = signup_at + timedelta(seconds=remaining * rng.uniform(0.3, 0.9))
            upper_bound = hard_delete_at

        follow_on_count = int(rng.poisson(3))
        if is_same_day:
            follow_on_count = max(follow_on_count, 2)
        if is_soft_delete:
            follow_on_count = max(follow_on_count, 1)

        timestamps: list[datetime] = []
        cursor_ts = signup_at
        for _ in range(follow_on_count):
            remaining_span = (upper_bound - cursor_ts).total_seconds()
            if remaining_span <= 60:
                break
            step_s = remaining_span * rng.uniform(0.05, 0.6)
            cursor_ts = cursor_ts + timedelta(seconds=step_s)
            timestamps.append(cursor_ts)

        if is_same_day and len(timestamps) >= 2:
            day: date = timestamps[0].date()

            def _time_on_day(day: date) -> datetime:
                return datetime.combine(
                    day,
                    time(
                        int(rng.integers(0, 24)),
                        int(rng.integers(0, 60)),
                        int(rng.integers(0, 60)),
                        int(rng.integers(0, 1_000_000)),
                    ),
                )

            t0, t1 = sorted([_time_on_day(day), _time_on_day(day)])
            if t0 == t1:
                t1 = t1 + timedelta(microseconds=1)
            timestamps[0], timestamps[1] = t0, t1
            timestamps = sorted(timestamps)

        change_seq = [signup_at, *timestamps]

        # emit signup event
        events.append(
            {
                "subscriber_id": sub_id,
                "change_event_id": f"subev_{len(events):08d}",
                "change_timestamp": signup_at,
                "change_type": "signup",
                "email": email,
                "display_name": display_name,
                "country_code": country_code,
                "acquisition_channel": acquisition_channel,
                "signup_date": signup_at.date(),
                "plan_tier": plan_tier,
                "status": status,
            }
        )

        current_plan_tier, current_status = plan_tier, status
        for k, ts in enumerate(timestamps):
            is_last = k == len(timestamps) - 1
            if is_soft_delete and is_last:
                current_status = str(rng.choice(SOFT_DELETE_STATUSES))
            else:
                if rng.random() < 0.5:
                    current_plan_tier = str(rng.choice(TIERS, p=[0.35, 0.35, 0.22, 0.08]))
                if rng.random() < 0.35:
                    current_status = str(rng.choice(["active", "paused", "trial"]))
                if rng.random() < 0.05:
                    first, last = _random_name(rng)
                    display_name = f"{first} {last}"
                if rng.random() < 0.03:
                    email = f"{email_local}.{k}@{EMAIL_DOMAINS[rng.integers(len(EMAIL_DOMAINS))]}"

            events.append(
                {
                    "subscriber_id": sub_id,
                    "change_event_id": f"subev_{len(events):08d}",
                    "change_timestamp": ts,
                    "change_type": "profile_update",
                    "email": email,
                    "display_name": display_name,
                    "country_code": country_code,
                    "acquisition_channel": acquisition_channel,
                    "signup_date": signup_at.date(),
                    "plan_tier": current_plan_tier,
                    "status": current_status,
                }
            )

        # midstream-join candidates: any non-final gap between two
        # consecutive versions, so a fact timestamped in that gap must
        # resolve to the OLDER of the two, not the newest version overall.
        if len(change_seq) >= 3:
            k = 0  # first non-final gap; deterministic choice, not random
            midstream_candidates.append(
                MidstreamCandidate(
                    subscriber_id=sub_id,
                    gap_start=np.datetime64(change_seq[k]),
                    gap_end=np.datetime64(change_seq[k + 1]),
                    newer_version_start=np.datetime64(change_seq[-1]),
                )
            )

        signup_epoch_s[i] = to_epoch_s(signup_at)
        effective_upper = change_seq[-1] if is_hard_delete else run.now
        upper_bound_epoch_s[i] = to_epoch_s(effective_upper)
        is_hard_deleted[i] = is_hard_delete
        if is_hard_delete:
            hard_delete_at_by_id[sub_id] = np.datetime64(effective_upper)

    late_arrival_ids = {subscriber_ids[i] for i in late_arrival_idx}
    hard_delete_ids = {subscriber_ids[i] for i in hard_delete_idx}
    soft_delete_ids = {subscriber_ids[i] for i in soft_delete_idx}
    same_day_ids = {subscriber_ids[i] for i in same_day_idx}

    result = SubscriberGenerationResult(
        subscriber_ids=subscriber_ids,
        signup_epoch_s=signup_epoch_s,
        upper_bound_epoch_s=upper_bound_epoch_s,
        is_hard_deleted=is_hard_deleted,
        late_arrival_ids=late_arrival_ids,
        hard_delete_ids=hard_delete_ids,
        soft_delete_ids=soft_delete_ids,
        same_day_ids=same_day_ids,
        midstream_candidates=midstream_candidates,
        total_events_emitted=len(events),
        hard_delete_at_by_id=hard_delete_at_by_id,
    )
    return result, events


def events_to_table(events: list[dict[str, object]]) -> pa.Table:
    return pa.Table.from_pylist(events)
