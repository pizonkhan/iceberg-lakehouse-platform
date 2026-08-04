"""Static reference data: plans, devices, and the title/genre bridge.

None of these three carry history (dim_plan is Type 3, dim_device is Type 1,
bridge_title_genre has no SCD tracking at all per .notes/modeling.md), so
each is generated as a single plain table, no batching, no change-event
stream. Row counts are small (tens to low thousands) so straightforward
per-row Python construction is used rather than numpy vectorization; at
this scale it is not the performance-sensitive path (that is playback.py).
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa

from generation.ids import build_id_pool
from generation.schemas import ReferenceData

TIERS = ["basic", "standard", "premium", "premium_plus"]
BILLING_PERIODS = ["monthly", "annual"]
VIDEO_QUALITY_BY_TIER = {
    "basic": "sd",
    "standard": "hd",
    "premium": "uhd",
    "premium_plus": "uhd",
}
STREAMS_BY_TIER = {
    "basic": 1,
    "standard": 2,
    "premium": 4,
    "premium_plus": 6,
}
BASE_PRICE_BY_TIER = {
    "basic": 6.99,
    "standard": 12.99,
    "premium": 17.99,
    "premium_plus": 22.99,
}

DEVICE_TYPES = ["tv", "mobile", "tablet", "web", "console"]
DEVICE_TYPE_WEIGHTS = [0.30, 0.32, 0.13, 0.18, 0.07]
MANUFACTURERS_BY_TYPE = {
    "tv": ["Samsung", "LG", "Sony", "Vizio", "TCL", "Roku"],
    "mobile": ["Apple", "Samsung", "Google", "OnePlus", "Xiaomi"],
    "tablet": ["Apple", "Samsung", "Amazon", "Lenovo"],
    "web": ["Generic"],
    "console": ["Sony", "Microsoft", "Nintendo"],
}
OS_BY_TYPE = {
    "tv": ["tizen", "webos", "androidtv", "roku_os", "vidaa"],
    "mobile": ["ios", "android"],
    "tablet": ["ios", "android", "fire_os"],
    "web": ["chrome", "safari", "firefox", "edge"],
    "console": ["playstation_os", "xbox_os", "horizon"],
}

GENRE_NAMES = [
    "action",
    "adventure",
    "animation",
    "comedy",
    "crime",
    "documentary",
    "drama",
    "family",
    "fantasy",
    "horror",
    "mystery",
    "reality",
    "romance",
    "sci_fi",
    "thriller",
    "war",
    "western",
    "music",
]


def generate_plans(plan_count: int, rng: np.random.Generator) -> pa.Table:
    plan_ids = build_id_pool("plan_", plan_count, 2)
    rows = []
    for i in range(plan_count):
        tier = TIERS[i % len(TIERS)]
        period = BILLING_PERIODS[(i // len(TIERS)) % len(BILLING_PERIODS)]
        base_price = BASE_PRICE_BY_TIER[tier]
        # annual plans priced roughly 10 months for 12, small per-plan jitter
        jitter = rng.uniform(-0.5, 0.5)
        price = round(base_price * 10 if period == "annual" else base_price + jitter, 2)
        rows.append(
            {
                "plan_id": plan_ids[i],
                "plan_name": f"{tier.replace('_', ' ').title()} ({period})",
                "current_tier": tier,
                "current_price_usd": max(price, 0.99),
                "billing_period": period,
                "max_concurrent_streams": STREAMS_BY_TIER[tier],
                "video_quality": VIDEO_QUALITY_BY_TIER[tier],
                "has_ads": tier == "basic" and rng.random() < 0.5,
                "supports_downloads": tier != "basic",
            }
        )
    return pa.Table.from_pylist(rows)


def generate_devices(device_count: int, rng: np.random.Generator) -> pa.Table:
    device_ids = build_id_pool("dev_", device_count, 5)
    type_idx = rng.choice(len(DEVICE_TYPES), size=device_count, p=DEVICE_TYPE_WEIGHTS)
    rows = []
    for i in range(device_count):
        device_type = DEVICE_TYPES[type_idx[i]]
        manufacturer = rng.choice(MANUFACTURERS_BY_TYPE[device_type])
        os_name = rng.choice(OS_BY_TYPE[device_type])
        rows.append(
            {
                "device_id": device_ids[i],
                "device_type": device_type,
                "manufacturer": str(manufacturer),
                "model_name": f"{manufacturer}-{rng.integers(100, 9999)}",
                "os_name": str(os_name),
                "is_mobile": device_type in ("mobile", "tablet"),
            }
        )
    return pa.Table.from_pylist(rows)


def generate_title_genre_bridge(title_ids: list[str], rng: np.random.Generator) -> pa.Table:
    """One to four genres per title, weights rounded to 4dp and summing to
    exactly 1.0000 (last weight absorbs the rounding residual so the sum
    test in .notes/modeling.md passes exactly, not approximately).
    """
    rows = []
    for title_id in title_ids:
        n_genres = int(rng.integers(1, 5))
        genres = list(rng.choice(GENRE_NAMES, size=n_genres, replace=False))
        raw_weights = rng.dirichlet(np.ones(n_genres))
        weights = np.round(raw_weights, 4)
        weights[-1] = round(1.0 - weights[:-1].sum(), 4)
        for genre_name, weight in zip(genres, weights, strict=True):
            rows.append(
                {
                    "title_id": title_id,
                    "genre_name": str(genre_name),
                    "allocation_weight": float(weight),
                }
            )
    return pa.Table.from_pylist(rows)


def generate_reference_data(
    plan_count: int, device_count: int, rng: np.random.Generator
) -> ReferenceData:
    return ReferenceData(
        plan_ids=build_id_pool("plan_", plan_count, 2),
        device_ids=build_id_pool("dev_", device_count, 5),
    )
