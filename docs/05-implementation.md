# Implementation

A guided tour of how this pipeline actually works, medallion layer by medallion layer, with real file paths you can open alongside this document. Nothing here is aspirational: every mechanism described is built, and the row counts and timings come from real runs against the local stack (see `.notes/decisions.md` for the full trail).

## The stack and how the pieces connect

Five services, all local Docker, defined in `infra/docker-compose.yml`:

- **MinIO** (`minio/minio:RELEASE.2025-09-07T16-13-09Z`) is the object store. Every Iceberg data file, manifest, and metadata.json lands in the `warehouse` bucket.
- **Postgres** (`postgres:16.14`) backs Nessie's JDBC version store. It holds Nessie's commit graph, not any table data itself.
- **Nessie** (`ghcr.io/projectnessie/nessie:0.108.4-java`) is the Iceberg REST catalog, chosen specifically for catalog-wide, git-like branching (see `.notes/decisions.md`, 2026-08-03): a branch covers every table at once, not one table's snapshot refs the way Polaris or Glue would give you. That property is what makes write-audit-publish possible later in this tour.
- **Trino** (`trinodb/trino:483`) is the query engine everything reads and writes through: dbt-trino compiles to Trino SQL, and Trino's Iceberg connector talks to Nessie's REST catalog and MinIO directly. It is a single coordinator-and-worker node, capped at `query.max-memory-per-node=1.5GB` (`infra/trino/etc/config.properties`). That cap shows up repeatedly below; it is the single most consequential infrastructure fact in this project.
- **dlt** runs on the host (not a Docker service) and loads bronze. **dbt-trino** also runs on the host and builds silver and gold. **Dagster** runs on the host too and orchestrates both.

Everything downstream of the stack itself is: `generation/` produces synthetic parquet, `ingestion/` (dlt) loads it into bronze, `transform/lakehouse/` (dbt-trino) builds silver and gold, `orchestration/` (Dagster) wraps the dbt build and the bronze load as one asset graph, and `ops/` holds two operational demonstrations (write-audit-publish and time travel) that exercise Nessie's branching directly.

## Generation to bronze

`generation/generate.py` is the seed-driven synthetic data generator: a fixed `SEED` in `generation/config.py` drives everything, so a full regeneration reproduces byte-identical row counts and pathologies every time (verified directly: a full-scale rerun at 103.5s produced identical counts to the original 123.6s run, per `.notes/failures.md`). It writes parquet under `generation/output/<entity>/`, one directory per source entity (`plans`, `devices`, `title_events`, `title_genres`, `subscriber_events`, `signup_funnel`, `billing_ledger`, `watchlist_adds`, `playback_sessions`), gitignored because the full-scale run is about 3GB.

Bronze ingestion is `ingestion/pipeline.py`, run as `uv run python -m ingestion.pipeline`. It uses dlt 1.29.1 with `pyiceberg` under the hood (`table_format="iceberg"` on dlt's `filesystem` destination; there is no dedicated dlt Iceberg destination package). `ingestion/sources.py` defines one dlt resource per entity: each resource reads every parquet file under its `generation/output/<entity>/` directory through DuckDB (vectorized, streaming Arrow record batches), attaches four metadata columns (`_source_file`, `_ingested_at`, `_batch_id`, `_payload_hash`), and yields to dlt for an append-only write into `iceberg.bronze.bronze_<entity>`.

### Idempotency and replayability

The unit of replay-safety is the **file**, not the row (`.notes/decisions.md`, 2026-08-04). `ingestion/sources.py`'s `make_entity_resource` tracks which relative file paths it has already loaded in dlt's own resource state (`dlt.current.resource_state()`, key `processed_files`), and skips anything already in that list. This is a deliberate boundary, not an oversight: bronze's job is to preserve source rows verbatim, and deduplicating on an inferred key at this layer would mean bronze making a business-key decision that belongs to silver.

The property that makes this trustworthy on a fresh clone is where that state actually lives. dlt's pipeline state is cached locally under `~/.dlt/pipelines/`, machine-specific, but it is also written into the destination itself (`s3://warehouse/bronze/_dlt_pipeline_state/...`) and restored from there at the start of every run. This was proven, not just read off dlt's docs: the local state directory was deleted to simulate a fresh clone against a warehouse that already had bronze data, and the next run still correctly skipped the already-loaded files with row counts unchanged. A genuinely empty warehouse (`make clean && make up`) has no state anywhere and reprocesses every file from scratch, the same full-replay behavior by construction rather than a separate code path.

Two other things worth knowing if you're reading `ingestion/`:

- `ingestion/network.py` patches `socket.getaddrinfo` for the life of the ingestion process so the container-network hostnames `minio` and `nessie` resolve to `127.0.0.1`, because Nessie hands pyiceberg back the compose-internal S3 endpoint (`http://minio:9000`) regardless of what the host-side client passed, and ingestion runs on the host, not in a container.
- `_payload_hash` is computed per file (via `ingestion/hashing.py`'s `payload_hash_expression`, a vectorized DuckDB SQL expression, not a Python loop) over that file's own actual columns, which is why the mid-stream schema drift in `playback_sessions` (one file gains a `playback_quality` column partway through the source data) needs no reconciliation: bronze never compares hashes across files.

Production ingestion of the full dataset (~123M rows across 9 tables) ran in about 3 minutes 50 seconds end to end, landing 5.1 GiB in MinIO, almost all of it in `bronze_playback_sessions` (120,000,300 rows). Re-running the full pipeline afterward loaded zero new packages, the idempotency proof at production scale, not just in a smoke test.

## Silver: dedup and the quality gate

Silver models live in `transform/lakehouse/models/intermediate/`, named `silver_<source>` (staging models one layer up are `stg_<source>`). `dbt_project.yml` materializes both as real tables, not ephemeral CTEs: at up to 120M rows, an ephemeral `silver_playback_sessions` would recompute its full CTE on every downstream `ref()`.

### Dedup tie-break

Two silver models carry real bronze-side duplication: `silver_billing_ledger` (1,515,049 bronze rows to 1,500,100 distinct `billing_transaction_id`, 14,949 duplicated) and `silver_signup_funnel` (70,701 to 70,000 distinct, 701 duplicated). Both are deduplicated the same way: `row_number()` partitioned by the business key, ordered by `_ingested_at desc, _batch_id asc`, keep rank 1. Every duplicate group in the real data turned out to be exactly size 2 with byte-identical `_payload_hash`, a true upstream retry replay rather than two different payloads racing for the same id, so the tie-break rule only needs to be deterministic, not discriminating between the copies. The same defensive dedup pattern (by `change_event_id`) is applied to `silver_subscriber_events` and `silver_title_events` even though bronze currently has zero duplicates there, on the reasoning that a future re-ingestion could introduce one and the model should not silently assume it never will.

### The playback quality gate

`silver_playback_sessions.sql` and its sibling `silver_playback_sessions_rejected.sql` route rows on three malformation checks, all defined once in `transform/lakehouse/macros/playback_malformed_predicate.sql`:

```sql
(watch_duration_seconds < 0 or session_ended_at < session_started_at or session_started_at > _ingested_at)
```

These are mutually exclusive on the real data and sum exactly to the pathology manifest's 360,201 malformed rows: 121,063 negative duration, 120,110 ended-before-started, 119,028 future timestamp. The future-timestamp check deliberately compares `session_started_at` against the row's own `_ingested_at`, not a literal date or `current_timestamp`, so the check is reproducible on every rebuild regardless of when it happens to run.

The malformed-row exclusion is written as three direct column comparisons, inlined into the `WHERE` clause via the macro, specifically because a derived `rejection_reason` column (a `CASE` expression, then `WHERE rejection_reason IS NULL`) defeats Trino's predicate pushdown to the Iceberg connector on a table this size, which was enough by itself to exceed the 1.5GB per-node memory cap. This is one instance of a broader fight documented in `.notes/decisions.md`: getting the two quality-gated playback models to build at all against real data took real troubleshooting (narrowing projected columns, splitting a three-way `UNION ALL` into three separately materialized `int_playback_rejected_*` tables, tuning `profiles.yml` session properties, isolating each heavy build into its own dbt invocation). None of that is exotic engine-tuning trivia; it is the direct consequence of running a genuinely large fact table on a single 1.5GB-capped Trino node, and it recurs at gold too (see the `fct_playback_events` section below).

One more thing worth knowing: there is no dbt-native `unique` test on `playback_session_id` at either the silver or gold layer. A plain `COUNT(DISTINCT playback_session_id)` over ~120M rows exceeds the memory cap outright, and attempting the equivalent test query directly crashed the Trino coordinator container, not just failed the query. Uniqueness was verified manually instead (a 44-way monthly-chunked scan, zero duplicates across all 119,640,099 fact rows at gold), documented as a real, deliberate gap in `.notes/open-questions.md` rather than silently skipped.

## Gold: dimensions

Gold models live in `transform/lakehouse/models/marts/dimensions/` and `.../facts/`. A gold model reading directly from bronze is a rejected architecture violation per `CLAUDE.md`; every dimension and fact reads from silver.

### dim_subscriber.sql: Type 6 hybrid SCD

`dim_subscriber.sql` is the most mechanically involved model in the project. `silver_subscriber_events` is a change-event stream (199,928 events for 50,000 subscribers, one row per profile change, not one row per subscriber), so the model has to do its own versioning from scratch:

1. **Status remapping happens first**, before anything else touches `status`: silver carries the source vocabulary verbatim (`trial`, `active`, `paused`, `cancelled`, `deleted`), and the `raw_events` CTE maps `cancelled`/`deleted` onto the gold vocabulary's `churned` before row-hashing or versioning ever sees the column. Without this remap, `churned` would never appear in the built table and `churn_date_key` would be permanently null (verified: silver has exactly 150 `cancelled`/`deleted` events, and gold shows exactly 150 rows with `status = 'churned'`).
2. **Version boundaries** are detected from a `row_hash` over `(plan_tier, status)` (`dbt_utils.generate_surrogate_key`), with three independent running counters computed via window functions: `version_group` (increments on any `row_hash` change), `plan_segment` (increments only when `plan_tier` itself changes), and `status_segment` (increments only when `status` itself changes). Runs of unchanged `row_hash` collapse into a single version.
3. **Type 3's `previous_plan_tier`** is implemented as a "plan segment cursor," not a plain `lag()` over versions: it has to stay unchanged across a run of status-only versions between two real plan changes. The model computes a `plan_prev_by_segment` CTE (`lag(plan_tier)` over distinct `(subscriber_id, plan_segment, plan_tier)`), then broadcasts that value back to every version row sharing the segment via a join on `plan_segment`.
4. **`churn_date_key`** works the same way, a "status segment cursor" (`status_segment_summary` / `latest_status_segment` / `churn_lookup`): it mirrors the `effective_from` of the subscriber's current status segment onto every row, but only when that segment's status is `churned`, and resets to null the instant the subscriber leaves that status. Because this model is full-refresh, only the *final* status segment can ever produce a non-null value in a given build, since a full refresh only ever materializes final state.
5. **Late-arriving subscriber self-heal** (`fact_subscriber_ids` / `late_arriving_ids` / `inferred_rows`): every build unions in a synthetic row (`is_inferred = true`, `effective_from = 1900-01-01`, `plan_tier`/`status = 'unknown'`) for any `subscriber_id` referenced by `silver_playback_sessions`, `silver_billing_ledger`, or `silver_watchlist_adds` but absent from `silver_subscriber_events`. Because this is a full-refresh model rather than an incremental merge, there is no persisted prior state to backfill in place; the model instead only ever synthesizes an inferred row for a `subscriber_id` that is *completely* absent from `silver_subscriber_events` at build time, and the moment real profile events exist, that id flows through the normal versioning path instead. Currently a no-op on the real data (zero such subscribers exist), kept as a real contract regardless.

The unknown member row (`subscriber_sk = md5('-1')`) is distinct from an inferred row: it exists so a fact FK that cannot resolve *any* subscriber still has somewhere to point, and it is always current, spanning `1900-01-01` to `9999-12-31`.

### dim_title.sql: pure Type 2

`dim_title.sql` is a simpler pattern, worth reading right after `dim_subscriber.sql` to see the same SCD mechanics without the Type 3/6 layering. A version begins whenever any of seven tracked attributes changes (`title_name`, `content_type`, `release_year`, `runtime_minutes`, `maturity_rating`, `original_language`, `is_original`); `row_hash` is computed over all seven, and a version boundary opens in the `changes` CTE only where `row_hash` differs from `lag(row_hash)` for the same title. `effective_to` is `coalesce(lead(effective_from), '9999-12-31 23:59:59.999999')`. There is no self-heal mechanism here, deliberately: the model's own header comment states the design assumption plainly, "titles arrive from a controlled catalog feed ahead of playback."

That assumption is worth pausing on, because it did not hold in the real generated data, and it is a genuinely interesting story about how a generator bug propagates all the way to a fact table's join-miss rate. `generate_titles()` originally drew each title's `catalog_add_at` from the same `[platform_launch, now)` span that `generate_playback_events()` drew session timestamps from, with no dependency between which title a playback row references and that title's own `catalog_add_at`. The result: 4,833 of 5,000 titles had at least one playback session timestamped *before* their own first catalog addition, and `fct_playback_events` resolved `title_sk` to the unknown member on 20,646,958 of 119,640,099 rows (17.3%). This was first found and root-caused correctly, then shipped anyway as "implemented exactly as specified, flagged in open-questions.md," which `.notes/failures.md` calls out directly as the actual mistake: root-causing a defect is not the same as fixing it. The real fix touched the generator, not the dimension: `generation/config.py` gained `CATALOG_SEED_LEAD_TIME` (180 days) and `CATALOG_SEED_BUFFER` (1 day), and `generate_titles()` now draws `catalog_add_at` from a window that ends strictly before `platform_launch`, guaranteeing by construction (not by chance) that no title's catalog entry can postdate any playback session referencing it. After regenerating and rebuilding, the unknown-member rate on `title_sk` dropped from 17.3% to exactly zero.

### dim_plan and dim_device

`dim_plan.sql` (Type 3) and `dim_device.sql` (Type 1) are both full-refresh tables built on the same unknown-member convention (`subscriber_sk = md5('-1')` pattern generalized: `dbt_utils.generate_surrogate_key` over the natural key, or over a sentinel combination when there is no natural key, as `dim_payment_method` needs since it is a junk dimension with none). They're smaller and mechanically simpler than `dim_subscriber`/`dim_title`; read them for the unknown-row convention if you need it, not for SCD mechanics.

## Gold: incremental facts

Four of the six fact models converted from full-refresh `table` materializations to `materialized='incremental', incremental_strategy='merge'` Iceberg MERGE. All four share the same core watermark design and hit overlapping engine limitations along the way; `fct_playback_events` diverges from the other three in a way that is worth understanding on its own terms.

### The shared design: an `_ingested_at` watermark, not event time

Every incremental fact filters on bronze's `_ingested_at` (carried through silver unchanged), never on the fact's own business event time (`added_at`, `transaction_posted_at`, `session_started_at`). The reasoning is the same in all four models: silver is rebuilt full-refresh every run but preserves each row's original `_ingested_at`, so `_ingested_at > watermark` correctly identifies rows genuinely new to bronze on this run, regardless of how old their event time happens to be. A watermark on event time would silently and permanently drop any row that arrives late relative to data already scanned, exactly the failure mode `fct_playback_events`' out-of-order arrival pathology (3% of playback rows deliberately spliced 3 to 15 batches later than their true chronological slice, `generation/playback.py`) is built to expose.

### The dbt-trino correlated-subquery problem

The naive watermark, `where _ingested_at > (select max(_ingested_at) from {{ this }})` inline in the model, does not work on this stack. It fails with `TrinoUserError(NOT_SUPPORTED, "Given correlated subquery is not supported")`. The root cause: dbt-trino's merge strategy compiles the incremental filter into a view that becomes the MERGE statement's `USING` source, so the compiled SQL is literally `MERGE INTO target USING (SELECT ... WHERE x > (SELECT max(x) FROM target)) AS src ON ...`, a scalar subquery against the MERGE's own target table, from inside that target's own source. Trino's planner rejects that shape outright.

The fix used by `fct_billing_transactions.sql` and `fct_watchlist_adds.sql`: resolve the watermark to a plain literal via a `run_query` pre-query (guarded by `{% if execute %}`), before the main SQL is assembled, so the compiled MERGE source never references the target table at all, just a literal timestamp:

```sql
{% set get_max_ingested_at_query %}
    select coalesce(max(sbl._ingested_at), timestamp '1900-01-01 00:00:00.000000') as max_ingested_at
    from {{ ref('silver_billing_ledger') }} as sbl
    inner join {{ this }} as f
        on f.billing_transaction_id = sbl.billing_transaction_id
{% endset %}
{% if execute %}
    {% set max_ingested_at_value = run_query(get_max_ingested_at_query).columns['max_ingested_at'].values()[0] %}
{% endif %}
```

Note the join back to silver rather than a direct `select max(_ingested_at) from {{ this }}`: the gold fact contract (`.notes/modeling.md`) deliberately excludes `_ingested_at`, only `loaded_at`, so the column simply does not exist on the physical fact table to select from. The watermark is instead recovered by joining `{{ this }}` back to silver on the shared grain key and taking the max `_ingested_at` among matched rows, which is valid because silver always carries every row's true original `_ingested_at`, and MERGE only ever touches rows matched by that grain key.

### fct_playback_events: the more sophisticated solution, and why

`fct_playback_events.sql` needed a different fix than the join-based pattern above, and the reason is a genuinely interesting engineering story, not just a stylistic choice. The join-based recovery query, `{{ this }}` joined back to silver on the grain key, is itself a full self-join over the fact's own row data. For `fct_billing_transactions` (1.5M rows) and `fct_watchlist_adds` (750k rows) that join is cheap. For `fct_playback_events` at ~120M rows, it is exactly the shape of query this project had already twice confirmed crashes or exceeds the 1.5GB-per-node cap (the abandoned `unique` test on this same table, and this model's own checksum verification, see below). Copying the join-based fix onto this table would have made computing the watermark itself the single most expensive part of every incremental run, defeating the entire point of converting it to incremental in the first place.

The actual solution: read the watermark from the table's own Iceberg snapshot history instead of its row data.

```sql
{% set snapshots_relation = this.incorporate(path={"identifier": this.identifier ~ "$snapshots"}) %}
...
where _ingested_at > (
    select coalesce(max(committed_at), timestamp '1900-01-01 00:00:00.000000 UTC')
    from {{ snapshots_relation }}
)
```

`"fct_playback_events$snapshots"` is a distinct relation from `{{ this }}` (so it is not a self-reference, sidestepping the correlated-subquery problem entirely), and it reads pure Iceberg manifest metadata, the commit wall-clock time of every write this model has ever made, at zero row-scan cost regardless of the table's size. The correctness argument mirrors any processing-time watermark: `_ingested_at` is assigned by the ingestion process's own clock and is monotonically increasing across runs, so a later gold build's commit time is never earlier than the `_ingested_at` of bronze data already merged into it as of that commit.

This is worth calling out explicitly: the three smaller facts' fix (join `{{ this }}` back to silver) and `fct_playback_events`' fix (read `$snapshots`) solve the identical structural problem (avoid a runtime self-reference against the MERGE's own target) with genuinely different mechanisms, and the difference is not arbitrary. It exists because the join-based approach's cost scales with the fact table's row count, while the snapshot-metadata approach's cost does not scale with row count at all. Below some crossover, the extra conceptual complexity of the snapshot-metadata trick is not worth it (a plain join is simpler to read); above it, on a table already sitting at this Trino instance's memory ceiling for a bare full scan, the join-based approach is not just slower, it is not safe to run at all. `.notes/decisions.md` flags this directly for anyone tempted to unify all four facts onto one pattern: don't, the two smaller facts' fix is not safe to copy onto this table without modification.

One more memory lesson from this model, worth reading if you touch it: an early draft added `_ingested_at` to the base CTE's `SELECT` list (it is only needed in the `WHERE` clause) so it would ride through three downstream wildcard joins as an extra column on the ~120M-row scan. That single extra column pushed the full-refresh build from "fits" to a hard JVM `OutOfMemoryError`. SQL permits filtering on a column without selecting it, and that is exactly what the shipped model does: `_ingested_at` is read into the base CTE for the `WHERE` clause only, never added to the final `SELECT`.

### fct_daily_subscription_snapshot: the odd one out

`fct_daily_subscription_snapshot.sql` is the one incremental conversion with no source event row to watermark against at all, because it is a periodic snapshot: rows are generated by expanding the subscriber roster across a date range, not read one-for-one from a silver table. Its incremental design question is which *days* to (re)generate, not which rows are new. It uses a `bounds` CTE with three branches (first build/full-refresh spans all time, normal incremental spans a rolling window, backfill takes vars verbatim) and a bounded reprocessing window (`reprocess_window_days`, default 3) that re-touches the most recent N days of already-written history on every run, a deliberate, cheap insurance policy against a dimension or ledger correction landing after the original day was written, while leaving everything older than the window untouched. Full reasoning and a constructed proof scenario are in `.notes/decisions.md`.

### Backfills

Each incremental fact accepts `backfill_start`/`backfill_end` (or, for the snapshot fact, `backfill_start_date`/`backfill_end_date`) dbt vars that override the normal watermark filter and route through the identical MERGE path, never a drop-and-rebuild. See the Operations doc for real invocation examples.

## Dagster: asset-based orchestration

`orchestration/` wraps the whole build as a Dagster asset graph, run via `make orchestrate` (`uv run dagster dev -m orchestration.definitions`). Two pieces:

- `orchestration/assets/dbt_assets.py` defines `lakehouse_dbt_assets` via `dagster_dbt`'s `@dbt_assets` decorator, pointed at `transform/lakehouse/target/manifest.json`. Asset keys, upstream/downstream dependencies, and column schemas all come from that manifest, not a hand-built lineage graph. Selection is `resource_type:model,package:lakehouse`, which excludes the 30 bookkeeping models the `elementary` dbt package pulls in (test result history, alerting views), keeping the surfaced graph to this project's real 37 medallion models. Column-level lineage is attached at materialization time via `DbtCliInvocation.fetch_column_metadata(with_column_lineage=True)`, which queries the warehouse for built-model and upstream-ref column schemas and derives column-to-column lineage from the compiled SQL with `sqlglot`, not hand-mapped. Verified live against `dim_subscriber`: the captured lineage correctly shows `churn_date_key` depending on `silver_subscriber_events.changed_at` and `.status`, and `previous_plan_tier` depending on `.changed_at`, `.plan_tier`, and `.subscriber_id`, matching the model's actual window-function logic.
- `orchestration/assets/bronze.py` supplies the one asset dagster-dbt doesn't generate: a single `@multi_asset` (`bronze_ingestion`) with one output per source table, wrapping `ingestion/pipeline.py`'s real `run()` entrypoint directly rather than reimplementing it. Its asset keys (`AssetKey(["bronze", "bronze_plans"])`, etc.) are chosen to match exactly what dagster-dbt already computes for each dbt source declared in `transform/lakehouse/models/staging/sources.yml`, so the dbt-generated staging assets' upstream dependencies resolve to this asset with no manual key mapping.

The generator (`generation/`) is deliberately not wrapped as a Dagster asset: it is the one-time, seed-driven batch step that produced the fixed dataset every model and every number in this project's history assumes, and making it re-runnable upstream of bronze would invite silently regenerating that dataset and invalidating everything built against it.

`orchestration/assets/dbt_assets.py` and `orchestration/assets/bronze.py` are the two modules in this project that omit `from __future__ import annotations`, the one deliberate exception to an otherwise universal convention: Dagster 1.13's asset-decorator validation reads a function's `context` parameter annotation via plain `inspect.signature()`, and PEP 563 deferred evaluation turns that annotation into a literal string Dagster's identity check then always rejects (`.notes/surprises.md`).

The verified asset graph loads 46 assets (37 dbt models plus 9 bronze sources), matching expectation exactly (`docs/evidence/dagster/`).
