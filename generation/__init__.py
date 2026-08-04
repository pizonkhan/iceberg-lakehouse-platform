"""Synthetic source-system data generator for the lakehouse bronze layer.

Produces raw, source-shaped data (change-event streams, transaction logs)
for a later work package to ingest, not the conformed gold shape described
in .notes/modeling.md. Run via `uv run python -m generation.generate
--scale {small,full}`. All randomness is seeded from generation.config.SEED
through generation.rng.child_rng; nothing here is nondeterministic.
"""
