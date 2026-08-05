"""Shared fixtures for tests/unit/.

Everything under tests/unit/ exercises pure Python logic directly: no live
Docker stack (Trino, MinIO, Nessie), no network calls. Fixtures here build
small, in-memory inputs (RunConfig, synthetic numpy arrays) rather than
anything that touches generation/output/ or a real warehouse.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# generation/, ingestion/ and orchestration/ are run as `uv run python -m
# <package>.<module>` from the repo root in normal use (pyproject.toml pins
# `[tool.uv] package = false`, so nothing here is pip-installed). pytest's
# default rootdir insertion only adds tests/unit/ itself to sys.path (no
# __init__.py above it), so the repo root is added explicitly here rather
# than by editing the project's shared pyproject.toml pytest config.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from generation.config import RunConfig  # noqa: E402


@pytest.fixture
def small_run_config() -> RunConfig:
    """The project's own SMALL scale/pathology preset.

    Real config, not a hand-rolled stand-in: exercises the same seed,
    now/platform_launch bounds, and pathology fractions the small-scale
    validation run uses, just without ever calling a generator.
    """
    return RunConfig.for_scale("small")
