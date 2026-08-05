# ADR-002: Iceberg table spec version, v2 over v3

## Status

Accepted, 2026-08-03.

## Context

Iceberg's table spec version determines which format features a table can use. Core Iceberg 1.11
had already shipped v3 (deletion vectors, row lineage) by the time this project started, so v3 was
a live option, not a hypothetical future one. But a table spec version is only useful if every
engine this project actually uses can read and write it: Trino, dbt-trino, PyIceberg, and DuckDB
all needed to agree.

## Decision

Pin every table to Iceberg spec v2, not v3.

v2 is the only version where Trino, dbt-trino, PyIceberg, and DuckDB all fully agree, at the
versions pinned elsewhere in this project. v3 was noted as a possible DuckDB-only stretch item if
time allowed, never a commitment, and was not pursued.

## Alternatives Considered

- **v3 across the board.** Would have given access to deletion vectors and row lineage natively.
  Rejected because Trino and PyIceberg did not fully support v3 at the versions this project
  actually pinned, which would have made the multi-engine story (Trino for the dimensional build,
  PyIceberg for ingestion via dlt, DuckDB as a candidate fast/local target) unreliable in ways that
  would have shown up as silent incompatibilities rather than clean errors.
- **Mixed versions per table** (v3 where an engine supported it, v2 elsewhere). Not seriously
  pursued: it would have made the multi-engine compatibility story table-dependent instead of a
  single, checkable project-wide fact, for no real benefit at this project's scale.

## Consequences

- This project cannot use v3-only features (deletion vectors, native row lineage) anywhere,
  including on the incremental MERGE facts where deletion vectors would otherwise be a natural fit.
- This is a pin to revisit, not a permanent architectural stance: once dbt-trino and PyIceberg both
  cut releases with full v3 support, upgrading is a version-bump decision, not a redesign.
- Every downstream write path (dlt/pyiceberg ingestion, dbt-trino incremental MERGE, DuckDB reads)
  was verified against v2 specifically; a partial v3 migration later would need the same kind of
  verification repeated, not assumed to carry over.
