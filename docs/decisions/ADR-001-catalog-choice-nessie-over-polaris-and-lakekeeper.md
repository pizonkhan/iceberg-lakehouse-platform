# ADR-001: Catalog choice, Nessie over Polaris and Lakekeeper

## Status

Accepted, 2026-08-03.

## Context

This project needed an Iceberg catalog to sit in front of a MinIO-backed warehouse, and one of the
stated goals was demonstrating write-audit-publish (WAP): stage a load somewhere isolated, run
quality gates against it, and only expose it to readers once it passes. That story only works if
the catalog can isolate an entire batch of changes, potentially across many tables, and then either
publish or discard all of them as one unit.

Three catalogs were evaluated: Project Nessie, Apache Polaris, and Lakekeeper.

## Decision

Use Project Nessie (JDBC version store on the shared Postgres, MinIO-backed warehouse).

Nessie is the only one of the three with catalog-wide, git-style branching: a branch can span every
table registered under it, and a merge or rollback of that branch applies to all of them atomically.
That is a direct match for the WAP requirement as stated: stage a load on a branch, run `dbt test`
against it, merge to main only if the audit passes.

## Alternatives Considered

- **Apache Polaris.** The ecosystem-default catalog post-graduation, with the broadest tooling
  support and the safest long-term bet if this project only needed a plain Iceberg REST catalog.
  Rejected for this specific requirement because Polaris's branching is per-table snapshot refs
  only, not catalog-wide. A WAP demonstration touching more than one table would need to coordinate
  refs across tables by hand instead of getting atomicity from the catalog itself, which defeats
  the point of building the demonstration on catalog machinery rather than application logic.
- **Lakekeeper.** The leanest of the three (a Rust binary, smallest footprint, fastest to stand up
  inside the 16GB Docker budget this project was already protecting). Rejected because it has no
  branching model at all, single-table or catalog-wide. It would have been the easiest catalog to
  operate but could not produce the WAP demonstration this project needed at any scope.

## Consequences

- Nessie's REST catalog does not expose Iceberg-native snapshot lineage the normal way (see
  ADR-008): every commit writes a fresh version-0 `metadata.json` rather than chaining to the
  previous one, a deliberate tradeoff Nessie makes to preserve cross-table consistency guarantees.
  Anyone expecting `FOR VERSION AS OF` or `$snapshots` history to work the way it does on a plain
  Hadoop or Glue catalog hits this immediately; time travel and rollback had to be built against
  Nessie's own commit log instead.
- Trino has no session-level way to target a non-default Nessie ref (confirmed against Trino's own
  source and a filed GitHub issue asking for exactly that feature), so branch-scoped querying
  requires a separate Trino catalog registration per ref (see ADR-009), not a query-time parameter.
- Nessie 0.108.4 has real, confirmed defects that had to be worked around: it cannot merge into a
  branch whose common ancestor is the server's own genesis commit (every brand-new repository's
  first merge hits this), and project-wide dbt package hooks (elementary) writing to an unscoped
  schema name produce spurious merge conflicts on keys the actual work never touched. Both are
  documented and worked around in `ops/wap.py`, not fixed upstream.
- Choosing Nessie over the ecosystem-default Polaris is a bet against the grain of where broader
  tooling investment is likely to go; revisit if Polaris ever ships catalog-wide branching.
