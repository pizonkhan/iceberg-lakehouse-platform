"""Naive-datetime <-> epoch-seconds conversion.

Every timestamp in this package is a naive datetime (no tzinfo), matching
Trino TIMESTAMP(6) which carries no timezone. Converting through
datetime.timestamp() would silently apply the host's local timezone, which
is both wrong and non-reproducible across machines, so this module always
computes the offset from a fixed naive epoch instead.
"""

from __future__ import annotations

from datetime import datetime, timedelta

EPOCH: datetime = datetime(1970, 1, 1)


def to_epoch_s(moment: datetime) -> int:
    return int((moment - EPOCH).total_seconds())


def from_epoch_s(seconds: int | float) -> datetime:
    return EPOCH + timedelta(seconds=float(seconds))
