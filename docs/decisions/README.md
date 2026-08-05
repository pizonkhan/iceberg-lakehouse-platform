# Architecture Decision Records

Dated log of the non-obvious, load-bearing choices made while building this platform. Each ADR
states the real alternatives considered and why they lost, not a generic pros/cons list. Source
material is `.notes/decisions.md`, `.notes/failures.md`, and `.notes/surprises.md` (gitignored
working logs); these ADRs are the durable, reviewable record.

| # | Title | Summary |
|---|-------|---------|
| [001](ADR-001-catalog-choice-nessie-over-polaris-and-lakekeeper.md) | Catalog choice, Nessie over Polaris and Lakekeeper | Nessie is the only one of the three with catalog-wide branching, required for the write-audit-publish demonstration. |
| [002](ADR-002-iceberg-table-spec-v2-over-v3.md) | Iceberg table spec version, v2 over v3 | v2 is the only spec version Trino, dbt-trino, PyIceberg, and DuckDB all fully agree on at the versions this project pinned. |
| [003](ADR-003-local-only-deployment-no-cloud-path.md) | Local-only deployment, no cloud path | R2 and AWS S3 both carry real auto-billing risk with no hard stop; neither meets a literal $0 guarantee, so the cloud path was dropped entirely. |
| [004](ADR-004-ingestion-time-watermark-for-incremental-processing.md) | Ingestion-time watermark, not event-time | An event-time watermark would silently and permanently drop the dataset's deliberately injected out-of-order arrivals. |
| [005](ADR-005-incremental-conversion-crossover-point-for-facts.md) | Which facts went incremental, and why | Four facts converted to incremental MERGE, one fact and every dimension stayed full-refresh, on a measured crossover: incremental earns its cost once full-rebuild cost is large relative to per-run delta, or the table's size is unbounded over time. |
| [006](ADR-006-synthetic-seeded-data-generator-with-injected-pathologies.md) | Synthetic, seeded generator over real or scraped data | A deterministic generator with eleven deliberately injected pathologies, verifiable against a manifest, since no real dataset carries these failure modes labeled and guaranteed present. |
| [007](ADR-007-deterministic-hash-surrogate-keys.md) | Deterministic hash surrogate keys | md5 via `dbt_utils.generate_surrogate_key`, not a sequence (Trino over Iceberg has no sequence object) and not the natural key (cannot pin one version of a versioned dimension). |
| [008](ADR-008-nessie-native-time-travel-and-rollback.md) | Nessie-native time travel and rollback | Iceberg-native snapshot chaining does not work on this catalog by design; time travel and rollback are built against Nessie's own commit log and branch pointer instead, with a documented per-reference retention policy. |
| [009](ADR-009-trino-dynamic-catalog-registration-for-branch-access.md) | Trino dynamic catalog registration for branch access | Trino has no session-level way to target a non-default Nessie ref, so a Trino catalog is registered and dropped per branch at runtime instead. |
| [010](ADR-010-compile-time-watermark-resolution-for-trino-merge.md) | Compile-time watermark resolution for Trino MERGE | An inline correlated-subquery watermark fails on Trino's MERGE compilation; the watermark is resolved to a literal at compile time instead, via a join to silver or, at the largest fact's scale, via Iceberg snapshot metadata. |
| [011](ADR-011-isolated-scratch-schemas-for-demonstrations.md) | Isolated scratch schemas for demonstrations | Schema evolution, write-audit-publish, time travel, and fail-then-fix test proofs each run in their own dedicated schema or branch, never against the real dev tables. |
