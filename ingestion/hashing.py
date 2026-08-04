"""Row-content hashing for the bronze `_payload_hash` column.

Reuses the canonicalization convention already fixed for surrogate keys in
.notes/modeling.md: every column cast to VARCHAR, NULL replaced with the literal
'_null_', values joined with '||', in a fixed column order. That convention was
written for dimension surrogate keys, not for bronze, but reusing it here keeps one
hashing rule for the whole project instead of two. md5 for the same reason: it is
already the project's standard (dbt_utils' generate_surrogate_key default) and
reproduces identically across DuckDB, Trino, and PyIceberg. This is an audit/dedup
fingerprint, not a security control, so md5's collision weakness is irrelevant.
"""

from __future__ import annotations


def payload_hash_expression(columns: list[str]) -> str:
    """Build a DuckDB SQL expression hashing `columns` in the given order."""
    parts = [f"coalesce(\"{column}\"::VARCHAR, '_null_')" for column in columns]
    joined = " || '||' || ".join(parts)
    return f"md5({joined})"
