"""Batch Parquet writing and small manifest file helpers.

Manifests document exactly which constructed rows realize a pathology (see
.notes/decisions.md), so the tester work package can target them directly
instead of statistically searching for them.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def write_batch(table: pa.Table, directory: Path, part_index: int, prefix: str = "part") -> Path:
    """Write one Arrow table as its own Parquet file in `directory`,
    numbered so file order is stable and inspectable. Separate files per
    batch (rather than one giant Parquet file with many row groups) is
    deliberate: it lets pathology 9 (schema drift) express itself as
    genuinely different Parquet schemas across files, the shape a real
    partitioned source extract would take.
    """
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{prefix}-{part_index:05d}.parquet"
    pq.write_table(table, path, compression="zstd")
    return path


def write_manifest_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_manifest_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str))
