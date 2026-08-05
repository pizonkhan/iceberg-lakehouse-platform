# ADR-009: Trino dynamic catalog registration for Nessie branch access

## Status

Accepted, 2026-08-05.

## Context

The write-audit-publish work package needed Trino to run `dbt run` and `dbt test` against a
specific Nessie branch, not main, and then have that branch's changes become visible through the
ordinary main-scoped `iceberg` catalog only after a successful merge. Trino's
`iceberg.catalog.type=rest` connector exposes `iceberg.rest-catalog.uri` and related properties as
catalog-registration-time configuration only, confirmed against Trino 483's own documentation and a
filed Trino GitHub issue (#24134) asking for a session-level or per-query equivalent as an
unimplemented feature. There is no way to point an existing catalog at a different ref for the
duration of one query.

Nessie's own REST catalog protocol makes the ref part of the URL path itself
(`/iceberg/v1/config` reports `"nessie.prefix-pattern": "{ref}|{warehouse}"`), so the only lever
available, given Trino has no session-level option, is a separate catalog registration per ref.

## Decision

Enable Trino's dynamic catalog management (`catalog.management=dynamic` in
`infra/trino/etc/config.properties`) so `ops/wap.py` can issue `CREATE CATALOG` /
`DROP CATALOG` statements at runtime, registering a Trino catalog scoped to a specific Nessie branch
(`iceberg.rest-catalog.uri=http://nessie:19120/iceberg/<branch_name>`) for the life of one WAP run,
and dropping it again regardless of outcome.

This was verified live end to end before any of `ops/` was written: a real branch was created via
the Nessie REST API, a table created and a row inserted through a branch-scoped Trino catalog,
confirmed invisible from the main-scoped `iceberg` catalog, merged via the Nessie REST API, then
confirmed visible through the ordinary main-scoped catalog afterward.

## Alternatives Considered

- **A static catalog properties file per WAP run**, writing a new file into
  `infra/trino/etc/catalog/` and restarting the Trino container to pick it up. This is Trino's
  default, non-experimental catalog management mode (`catalog.management=static`), confirmed
  directly: a `CREATE CATALOG` attempt against the live server with the default configuration was
  correctly refused ("not supported by the static catalog store"). Rejected because it is slower
  (a full container restart per run), noisier for anyone else using the shared stack concurrently,
  and does not fit the "unique branch name per run" requirement nearly as cleanly as issuing and
  tearing down one SQL statement within the same process.
- **A second, permanently-registered Trino catalog per known branch pattern** (for example one
  static catalog for `wap_*` traffic in general). Not pursued: WAP branches are created with unique,
  run-specific names, so a fixed catalog registration could not target a specific run's branch
  without still needing some form of runtime reconfiguration, which brings back the same restart
  problem the static-file approach has.

## Consequences

- `catalog.management=dynamic` is Trino's own documented label for this feature and is explicitly
  still "experimental" upstream. This project accepted that status because the alternative was
  materially worse for this use case, not because the experimental label was judged low-risk in
  general.
- The Trino catalog registration is never itself treated as evidence and is dropped every time a WAP
  run ends, regardless of success or failure; only the underlying Nessie branch (kept by default on
  failure for human inspection, see ADR-008's retention policy) carries any lasting state.
- This mechanism is reused unmodified by the later time-travel work package (ADR-008): passing a ref
  string that already includes `@<commit-hash>` to the same catalog-registration function works with
  no new code, since the function never assumed anything about what the branch-name string contains.
- Every WAP or time-travel run now depends on this one config line being present in
  `infra/trino/etc/config.properties`; a fresh environment that skips this setting would fail at the
  first `CREATE CATALOG` attempt with the same "not supported by the static catalog store" error this
  project hit during verification.
