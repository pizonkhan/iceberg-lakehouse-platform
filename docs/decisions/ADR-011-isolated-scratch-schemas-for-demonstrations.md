# ADR-011: Isolated scratch schemas for demonstrations, never the real dev tables

## Status

Accepted, 2026-08-04.

## Context

Several work packages in this project needed to demonstrate or prove a mechanism against a real,
live Trino/Iceberg/Nessie/MinIO stack rather than a mock: schema evolution (adding, renaming,
widening, reordering, dropping columns; partition evolution), write-audit-publish (a clean load and
a deliberately bad load, both audited), time travel and rollback, and the test suite's fail-then-fix
proofs for grain violations and SCD interval integrity. Each of these needs a table that can be
corrupted, dropped, evolved, or rolled back on purpose. Every one of them explicitly rules out
touching the project's real gold and silver layers (`dev_dimensions`, `dev_facts`, `dev_silver`, or
the `bronze` schema) to do it, since those are being concurrently built and relied on by other work
in the same window.

## Decision

Every demonstration or destructive-proof work package builds its own dedicated, isolated schema
with purpose-built tables, never touching a real layer:

- Schema evolution: `iceberg.schema_evolution_demo`, two purpose-built tables
  (`demo_billing_events`, `demo_billing_events_partitioned`), shaped to resemble
  `fct_billing_transactions` / `fct_playback_events` but populated with seeded synthetic rows, not
  real data.
- Write-audit-publish: a standalone `wap_demo_dim` dbt model, isolated in its own `wap_demo` schema
  (`+schema: wap_demo` in `dbt_project.yml`), eight static rows, with the "bad load" scenario
  controlled by a dbt var (`wap_demo_inject_bad_row`) rather than a second model.
  Time travel: a dedicated Nessie branch (`time_travel_demo`) cut from main, with its own demo
  table (`demo_billing_batch`), never running its rollback against main itself.
- Test suite fail-then-fix proofs: a session-scoped `iceberg.test_scratch` schema
  (`s3://warehouse/test_scratch/`), created lazily by a pytest fixture and torn down per test via a
  function-scoped `scratch_table` fixture factory that drops the table even when the assertion mid-
  test fails.

## Alternatives Considered

- **Prove each mechanism directly against a real model** (for example demonstrate schema evolution
  by actually altering `dim_title`, or grain-violation detection by actually corrupting
  `dim_subscriber`). This is the most literal reading of "prove it against the real stack" and was
  explicitly ruled out by every relevant work package's own scope constraints: these tables are
  concurrently being built and depended on by other agents in the same window, and corrupting one on
  purpose, even temporarily, risks a real, hard-to-diagnose collision with unrelated work in
  progress.
- **A separate, throwaway Docker Compose stack per demonstration.** Would have given full isolation
  at the infrastructure level, not just the schema level. Rejected as disproportionate: every
  demonstration needed here is expressible as an isolated schema or branch on the existing stack,
  and standing up a second stack per demonstration would have cost meaningfully more setup and
  teardown complexity for no additional isolation benefit, since the actual risk being managed is
  data collision, not resource contention.
- **Mocking the catalog or query engine** instead of running against the real stack. Rejected
  project-wide, not just for this decision: every verification claim in this project's build history
  is backed by a real command run against the real Trino/Nessie/MinIO stack, specifically because
  several genuine defects (the Nessie genesis-commit merge failure, the elementary-hooks conflict,
  the correlated-subquery limitation) were only ever found by running the real thing, never by
  reasoning about it in advance.

## Consequences

- Demonstration schemas and tables are, by explicit choice, kept in place after their work package
  finishes rather than dropped: the schema-evolution work package's own reasoning (trivial footprint,
  fully isolated from every real layer, evidence files reference specific paths a later reviewer may
  want to re-run directly) became the precedent every later demonstration followed, including the
  time-travel demo branch. A reader of this repository will find these demo objects still present,
  not evidence of leftover debris.
- This pattern means the project now has four separate isolated namespaces
  (`schema_evolution_demo`, `wap_demo`, `time_travel_demo`, `test_scratch`) with no single shared
  convention for how they are created or torn down: schema evolution and WAP use plain dbt-managed
  schemas, time travel uses a Nessie branch, and the test suite uses a pytest fixture. A future
  demonstration needs to choose which of these shapes fits, not follow one uniform recipe.
- None of these namespaces are covered by the retention policy chosen for Nessie commits (ADR-008)
  except `time_travel_demo` specifically, which is named in that policy by pattern
  (`time_travel_demo=P7D`). `schema_evolution_demo` and `wap_demo` are plain schemas on main, so
  their storage is governed by main's own `P14D` cutoff, not a dedicated one; `test_scratch` is torn
  down per test and does not accumulate at all.
