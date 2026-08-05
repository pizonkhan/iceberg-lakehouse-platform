# Operations

A runbook for running, monitoring, backfilling, and recovering this stack. Grounded in what has actually happened on this project, including two real incidents, rather than generic advice.

## Deploying from nothing

The full sequence, in order, is what `make up && make seed && make build && make test` runs. Each target's real behavior (`Makefile`):

1. **`make up`**: `docker compose -f infra/docker-compose.yml --env-file .env up -d --wait`. Brings up Postgres, MinIO, `minio-init` (creates the `warehouse` bucket), Nessie, and Trino, in that dependency order, and waits for all healthchecks. Idempotent: a fast no-op if everything is already healthy. On a genuinely fresh clone this took about 18.5 seconds in the last verified fresh-clone run. First-ever boot on this project took five attempts before the stack came up clean at all (see Common failure modes below); once the compose file's known-good image pins and config are in place, it's reliable. Idle memory across all four services: about 1.25GB, well under the project's 16GB budget.

2. **`make generate`**: `uv run python -m generation.generate --scale $(SCALE)` (`SCALE` defaults to `full`), but only if `generation/output/run_summary.json` doesn't already exist; otherwise it skips with a log message. `generation/output/` is gitignored (up to 3GB at full scale), so a fresh clone always runs this the first time. Full-scale generation: about 123.6 seconds wall clock. Small-scale (`make generate SCALE=small`, useful for a fast smoke run): about 3.1 seconds.

3. **`make seed`**: depends on `up generate`, then runs `uv run python -m ingestion.pipeline`. Loads every parquet file under `generation/output/` into bronze. Full-scale ingestion (~123M rows across 9 tables): about 3 minutes 50 seconds end to end. Fails fast with a clear `RuntimeError` if `generation/output/` is missing, rather than silently loading zero rows.

4. **`make deps`**: `dbt deps` against `transform/lakehouse`. Installs `dbt_utils`, `dbt_expectations`, and `elementary` into `dbt_packages/`, which is gitignored (dbt's own convention). Both `build` and `test` depend on this target; skipping it is the single most common "works on my machine, fails on a fresh clone" trap this project has hit (see below).

5. **`make build`**: `dbt build --target trino` (via `deps`). Builds staging through gold. **Always pass `--target trino` implicitly by using this target**: the `duckdb` target exists in `transform/lakehouse/profiles.yml` but is not wired to read the Nessie REST catalog (`attach: None`) and fails on the very first model with `Binder Error: Catalog "iceberg" does not exist!`. At small scale in a fresh-clone check, full `dbt deps` + `dbt build` (356/356 pass) took about 37.5 seconds. At full scale, individual heavy models are the pacing item, not the aggregate: `fct_playback_events` alone is 242.68s full-refresh (11-21s steady-state incremental), and the two quality-gated playback intermediate models (`int_playback_rejected_ended_before_started`, `int_playback_rejected_future_timestamp`) are sensitive enough to the 1.5GB memory cap that they have needed isolated single-invocation runs against a freshly restarted Trino container during heavy verification passes.

6. **`make test`**: depends on `deps`, runs `uv run pytest tests/unit tests/integration` then `dbt test --target trino` (audit-only, matching the write/audit split `ops/wap.py` uses: `make test` assumes `make build` already materialized gold, it does not rebuild). Small-scale fresh-clone timing: about 25.8 seconds. Full pytest suite: 109 passed / 1 skipped. Full `dbt test`: 288/288 pass at the time this was last verified end to end (356/356 including `dbt build`'s own models plus tests together).

7. **`make orchestrate`** (optional, for interactive use): `mkdir -p .dagster_home && DAGSTER_HOME=.dagster_home uv run dagster dev -m orchestration.definitions`. Requires the stack up and a built dbt manifest (`transform/lakehouse/target/manifest.json`, produced by `make build` or a bare `dbt parse`); the Dagster assets read that manifest as-is rather than re-parsing it on every server start.

8. **`make docs`** (via `deps`): `dbt docs generate --target trino`, writing `transform/lakehouse/target/index.html`, `manifest.json` and `catalog.json`. `catalog.json` comes from querying the live warehouse for every model's real columns and types, so this needs `make build` to have already materialized gold, the same assumption `make test` makes; it does not build anything itself. `target/` is gitignored, so browse the generated site locally (`dbt docs serve` from `transform/lakehouse`, or a plain `python -m http.server` in `target/`) rather than expecting it committed. A static rendering of the real lineage graph (dbt's own DAG view is a client-side JavaScript app with no CLI image export) is committed at `docs/evidence/dbt-docs/lineage-graph.png`, generated straight from `manifest.json` by `docs/evidence/dbt-docs/render_lineage.py`; see `docs/evidence/dbt-docs/README-evidence-index.txt` for how to regenerate it.

A genuine fresh-clone run of the whole sequence at small scale, after the known gaps below were fixed, completed in under 2 minutes total. Full scale is the Makefile's real default and what a real onboarding run uses; it is meaningfully slower (generation alone is ~124s before ingestion or any build starts) but is the only scale every dimension and fact model has actually been built and verified against.

`make clean` (`docker compose down -v`) removes the named Postgres and MinIO volumes entirely. `make down` (no `-v`) stops containers but preserves data. **Never run `make clean` (or any bare `docker compose ... down -v` against `infra/docker-compose.yml`) without being certain which stack you're pointed at**: see the WAP incident below.

## Monitoring

If something looks wrong, check in this order:

1. **`docker ps`**: are all five containers (`postgres`, `minio`, `minio-init`, `nessie`, `trino`) up and healthy? `minio-init` is expected to exit 0 after creating the bucket; anything else still running or restarting is the first sign of trouble. `docker stats` shows live memory: Trino is capped at `mem_limit: 2560m` in `infra/docker-compose.yml`, Nessie at `1g`, Postgres and MinIO at `512m` each.

2. **Trino system tables**, via any Trino client (`docker exec -it <trino-container> trino`, or a plain Python/DBAPI session):
   - `SELECT * FROM system.runtime.queries WHERE state = 'RUNNING'`: what's actually executing right now, useful for spotting a query that's about to hit the memory cap or one that's hung.
   - `SELECT * FROM iceberg.dev_facts."fct_playback_events$snapshots"` (or any table's `$snapshots`/`$history`/`$files`): real Iceberg manifest metadata, cheap to query regardless of table size. Remember this stack's Nessie REST catalog only ever exposes the single current snapshot here (no parent chain); that's expected, not a sign of missing history, see Common failure modes.
   - `DESCRIBE iceberg.dev_facts.fct_playback_events`: confirm the physical column set matches the modeling contract if something's behaving unexpectedly around a watermark or a `COLUMN_NOT_FOUND` error.
   - Row counts as a sanity check against the known-good baseline: `fct_billing_transactions` 1,500,100, `dim_subscriber` 125,616, `fct_playback_events` 119,640,099, `bronze_playback_sessions` 120,000,300, `dim_plan` 31, `fct_daily_subscription_snapshot` 27,011,346, `fct_watchlist_adds` 750,000, `fct_signup_funnel` 70,000. Any of these drifting from the expected count (outside of a deliberate backfill or a genuine new incremental load) is worth investigating before anything else.

3. **Dagster's asset graph** (`make orchestrate`, then open the served webserver): the live GraphQL endpoint and asset graph UI show 46 assets (37 dbt models plus 9 bronze sources) grouped by medallion layer (staging / silver / dimensions / facts / bronze). A stale or missing asset here usually means the dbt manifest needs regenerating (`dbt parse` or `make build`), not a real orchestration bug. `dagster asset materialize --select <asset>` re-executes a real `dbt build --select <model>` against the live stack and is the fastest way to confirm one specific model builds cleanly in isolation.

## Backfilling

Four fact models support a `dbt vars`-driven backfill that reprocesses a specific range through the same MERGE path, never a drop-and-rebuild:

- **`fct_watchlist_adds`**: `backfill_start` and `backfill_end`, both required together (either alone, or during a first build/`--full-refresh`, is ignored).
  ```
  dbt build --select fct_watchlist_adds --target trino --vars \
    '{"backfill_start": "2026-08-04 00:00:00.000000", "backfill_end": "2026-08-05 00:00:00.000000"}'
  ```
- **`fct_billing_transactions`**: `backfill_start`/`backfill_end`, both optional independently (a one-sided "everything since X" or "everything before Y" is valid).
  ```
  dbt build --select fct_billing_transactions --target trino --vars \
    '{backfill_start: "2026-08-04 00:00:00.000000", backfill_end: "2026-08-05 00:00:00.000000"}'
  ```
- **`fct_playback_events`**: same independently-optional `backfill_start`/`backfill_end` style. **Be careful with wide windows on this table**: because a MERGE needs a `HashBuilderOperator` to match source against target in addition to the scan cost a plain CTAS already pays, a backfill window matching a large fraction of this table's ~120M rows can hit `EXCEEDED_LOCAL_MEMORY_LIMIT` even though the equivalent full-refresh CTAS over the same row count succeeds. A window covering this project's entire current bronze history (all rows currently share one `_ingested_at`) reliably fails; a narrow, genuinely partial window runs the identical MERGE path cheaply. Every failed attempt leaves the table fully intact (Iceberg's atomic commit guarantee, the same one demonstrated by the failure-injection test below), so a failed wide backfill is safe to retry narrower, not a data-loss risk.
  ```
  dbt build --select fct_playback_events --target trino --vars \
    '{backfill_start: "2026-08-04 00:00:00.000000", backfill_end: "2026-08-05 00:00:00.000000"}'
  ```
- **`fct_daily_subscription_snapshot`**: `backfill_start_date`/`backfill_end_date` (inclusive both ends, date-grained rather than timestamp-grained since this is a periodic snapshot). Use this to repair a gap or re-run a specific range beyond the normal 3-day rolling reprocess window.
  ```
  dbt build --select fct_daily_subscription_snapshot --target trino --vars \
    '{backfill_start_date: "2026-05-15", backfill_end_date: "2026-05-15"}'
  ```

Never pass `--full-refresh` alongside a backfill var: `--full-refresh` drops and rebuilds the whole table from scratch, which defeats the point of a targeted backfill and, on `fct_playback_events` specifically, costs several minutes instead of seconds.

## Recovering from a failure

Two real incidents on this project are worth knowing in detail, because they're the actual shape of failure this stack produces, not hypotheticals.

### Incident 1: the WAP CI test wiped the local warehouse

While locally verifying the write-audit-publish GitHub Actions workflow with `act`, the entire local dev stack's data was destroyed. Root cause: `act` mounts the host Docker daemon by default rather than an isolated one, and `infra/docker-compose.yml` hardcodes `name: iceberg-lakehouse` at the top of the file. The CI workflow's own teardown step (`docker compose -f infra/docker-compose.yml --env-file .env down -v`) is correct and necessary on a real, isolated GitHub-hosted runner, but run locally via `act` it tore down the exact same containers and volumes the real, already-running local stack was using. `docker volume ls` afterward showed both `iceberg-lakehouse_postgres-data` and `iceberg-lakehouse_minio-data` gone entirely: Nessie's whole version-store history and every table in MinIO (bronze, silver, dimensions, facts) were erased.

**Recovery, in order** (this is the actual playbook if you ever land in this state):
1. `make up`: fresh, empty `iceberg-lakehouse-*` volumes.
2. `uv run python -m ingestion.pipeline` (what `make seed` calls without the generate step, since `generation/output/` is gitignored but lives on disk, not inside any Docker volume, so it survives a volume wipe untouched). This is bronze ingestion's replayability guarantee actually being exercised for real, not just tested: it completed cleanly from the same source parquet.
3. A full `dbt build --target trino` to rebuild `dev_staging`/`dev_silver`/`dev_dimensions`/`dev_facts` from bronze.

The rebuild itself hit two secondary problems worth expecting if you're doing this under similar conditions: `int_playback_rejected_future_timestamp` and `int_playback_rejected_ended_before_started` failed with `CLUSTER_OUT_OF_MEMORY` (the known 1.5GB-per-query sensitivity on playback-scale scans, plausibly aggravated by concurrent load from a second `act` attempt running against the same host), and `fct_playback_events` plus two `fct_daily_subscription_snapshot` uniqueness tests failed with `RemoteDisconnected` at the exact moment a third `act` attempt tried and failed to bind a host port already in use. Fixed by rerunning just the failed models/tests (`--select <model>+ --threads 1`, with no other Docker activity running concurrently), and, for one model, restarting the Trino container first to clear accumulated heap pressure before retrying. A full `dbt build` was re-run afterward as a no-op-expected final check that everything, not just the specific things noticed as failed, was genuinely clean.

**The actual fix that prevents this from recurring**: `.github/workflows/wap-gate.yml` sets `COMPOSE_PROJECT_NAME: wap-ci-${{ github.run_id }}` at the workflow level, overriding the fixed project name for CI's own containers and volumes. Verified this actually works, not just reasoned about it: a subsequent local `act` run against the fixed workflow failed fast and safely on a host port conflict (127.0.0.1:9000 already bound by the real stack) rather than attaching to or tearing down anything real.

**The broader lesson, if you're about to dry-run a CI workflow locally against a compose file with a fixed project name**: check whether the workflow's own teardown step could collide with a real running stack on your machine *before* running it, not after. The risk here was fully inferable from information already in hand.

### Incident 2: proving Iceberg's atomic commit under a mid-merge kill

As part of a deliberate failure-injection exercise (not an accident), a real, live Trino MERGE query was killed mid-write against `fct_billing_transactions` to verify Iceberg's commit atomicity actually holds on this stack, rather than assuming it. Method, reusable as a template for anyone who wants to re-verify this:

1. Recorded a baseline: 1,500,100 rows, content checksum `BA98E50C4EF99C85` (an order-insensitive `checksum(md5(...))` over every column except `loaded_at`, which is wall-clock audit metadata excluded on purpose).
2. Used the model's own backfill mechanism with a window covering the whole dataset, which forces the MERGE to re-match and rewrite all 1,500,100 existing rows, a real substantial write, while being provably content-neutral (every column but `loaded_at` is a pure function of unchanged upstream data), so any checksum drift afterward could only mean real corruption.
3. Launched the backfill, polled `system.runtime.queries` for a `RUNNING` query referencing the fact table, and the instant one appeared, called `CALL system.runtime.kill_query(query_id => ..., message => ...)` and separately `SIGKILL`ed the dbt client process. Both matter: Trino can keep executing server-side after a client disconnects, so killing only the client process doesn't guarantee the write stops.
4. First attempt landed before any write began (query state `FAILED` / `ADMINISTRATIVELY_KILLED`, zero new Parquet files, table completely unchanged, the cleanest possible outcome but not the strongest proof).
5. Second attempt polled MinIO's `data/` directory directly for a new file to appear before killing, catching a genuinely in-flight write: a real Parquet file appeared, then the kill landed. Afterward: the file was still physically present in MinIO (Iceberg doesn't auto-delete uncommitted files on failure; that's what `remove_orphan_files` maintenance is for), but querying `iceberg.dev_facts."fct_billing_transactions$files"` for that path returned zero rows, meaning the current committed snapshot's manifests reference it nowhere. Table state immediately after: 1,500,100 rows, checksum unchanged, no duplication, and a plain `SELECT count(*)` succeeded throughout, proving the table was never locked or in a partial state.
6. Rerunning the identical backfill command to completion (the model's own documented recovery path, no special repair step) succeeded both times and matched the original baseline exactly.

**Takeaway for on-call**: if a dbt run or a MERGE gets killed (host restart, OOM, a `Ctrl-C`), the correct recovery is simply to rerun the same command. Iceberg's atomic commit means a killed write either never happened as far as any reader is concerned, or it's an orphaned file that costs nothing and can be cleaned up later with `ALTER TABLE ... EXECUTE remove_orphan_files` if you care about tidiness; either way there is no manual table surgery required. The one thing dbt-trino itself does *not* clean up on a killed run is its own intermediate `__dbt_tmp` relation: if a retry fails with `TABLE_ALREADY_EXISTS: ... __dbt_tmp`, drop it directly (`DROP TABLE`/`DROP VIEW iceberg.<schema>.<model>__dbt_tmp`) before retrying, the same "clean up through the catalog, never by deleting object-store files directly" rule as the bronze incident below.

## Common failure modes

### Nessie and MinIO image registry surprises

Both images were guessed from their GitHub release tag names on first boot, and both guesses were wrong:

- `projectnessie/nessie` on Docker Hub is stale (last pushed 2024-01). The real image is `ghcr.io/projectnessie/nessie:0.108.4-java`, findable only in the GitHub release notes body, not from the tag name pattern.
- `minio/minio` on Docker Hub stopped getting new tags after `RELEASE.2025-09-07`; MinIO shifted toward a commercial product with source-only community builds after that. This stack is pinned to the last tag Docker Hub actually has, `RELEASE.2025-09-07T16-13-09Z`, a known version lag, not a bug.
- Nessie's healthcheck must hit `http://localhost:9000/q/health/ready` (the Quarkus management port, not published to the host but reachable inside the container), not the main API port 19120: Nessie splits health/metrics onto a separate port.
- Nessie's S3 credentials are not a plain config value: `nessie.catalog.service.s3.default-options.access-key` is a `urn:` pointing at a separately declared secret block (`nessie.catalog.secrets.<name>.name`/`.secret`). This shape only appears in Nessie's own example compose files, not in the prose config docs.
- `quarkus.datasource.*` (unnamed) is deprecated for Nessie's JDBC version store; use `quarkus.datasource.postgresql.*` plus `nessie.version.store.persist.jdbc.datasource=postgresql`.

**Lesson if you're touching `infra/docker-compose.yml` or bumping an image pin**: don't trust a GitHub release tag name to predict a container registry tag or a Quarkus-style dotted config property shape. Read the project's own example compose file, not just prose docs.

### The Trino single-node memory ceiling

`query.max-memory-per-node=1.5GB` (`infra/trino/etc/config.properties`) is the single most consequential constraint in this project. What triggers it, confirmed by direct testing, not supposition:

- A bare full-width scan of `bronze_playback_sessions` or `stg_playback_sessions` (~16 columns, ~120M rows) costs approximately 1.47-1.50GB regardless of predicate selectivity or query shape, right at the ceiling, so success or failure can vary run to run for structurally identical queries.
- A derived, `CASE`-expression-based filter column defeats predicate pushdown to the Iceberg connector; the same filter written as direct column comparisons in the `WHERE` clause does not. This is why `transform/lakehouse/macros/playback_malformed_predicate.sql` is inlined as three raw comparisons rather than a `rejection_reason` column.
- A single query `UNION ALL`-ing multiple scans of the same large table reliably exceeds the cap even when each branch's predicate alone fits as its own query. Fix pattern: materialize each branch as its own small intermediate table and union the small results, not the wide scans.
- Date-range chunking does **not** reduce scanned volume on `playback_sessions`: `EXPLAIN (TYPE IO)` on a query bounded to a five-month range still estimates scanning all 120,000,300 rows, because the generator's deliberate out-of-order pathology (3% of rows spliced 3-15 batches later than their true chronological slice) means file-level min/max stats on `session_started_at` are never tight enough for Iceberg to prune files.
- What does work: narrowing the projected column set (fewer columns = less memory per row), splitting heavy scans into isolated single dbt invocations with a fresh Trino container beforehand rather than chaining them in one `dbt build`, and the `session_properties` tuning already applied in `profiles.yml`'s `trino` target (`task_concurrency`, `initial_splits_per_node`, `scale_writers`, writer count bounds).
- A MERGE needs more memory than an equivalent CTAS at the same row count, because it adds a `HashBuilderOperator` to match source against target on top of the scan cost. This is why wide `fct_playback_events` backfill windows can fail even though the full-refresh CTAS at the same row count succeeds (see Backfilling above).
- There is no dbt-native uniqueness test on `playback_session_id` at ~120M rows: the equivalent query has crashed the Trino coordinator container outright, not just failed gracefully. If you're tempted to add one, don't; verify uniqueness manually in date-chunked pieces instead (see `.notes/open-questions.md` for the documented gap and the manual verification method actually used).

**If you hit `EXCEEDED_LOCAL_MEMORY_LIMIT` or a JVM `OutOfMemoryError` on Trino**: restart the Trino container to clear accumulated heap pressure before retrying (`docker restart <trino-container>`, or `docker compose -f infra/docker-compose.yml restart trino`), narrow whatever column projection or predicate shape triggered it, and if it's a large scan, check whether concurrent load from another process is a contributing factor before assuming it's a regression in the query itself.

### The dbt-trino MERGE self-reference limitation

Any incremental model's watermark filter that reads `max(<col>) from {{ this }}` inline will fail on this stack with `TrinoUserError(NOT_SUPPORTED, "Given correlated subquery is not supported")`, because dbt-trino's merge strategy compiles the incremental filter into the MERGE statement's own `USING` source, making a direct read of `{{ this }}` from inside that source a self-reference Trino's planner rejects outright. Two working patterns exist in this codebase, and which one to use depends on table size:

- **Small-to-medium facts** (`fct_billing_transactions`, `fct_watchlist_adds`): resolve the watermark via a `run_query` pre-query (`{% if execute %}`-guarded) that joins the fact back to silver on the shared grain key, recovering `_ingested_at` even though it's not part of the gold column contract.
- **`fct_playback_events`** specifically (~120M rows): the join-based pre-query above is itself a full self-join over ~120M rows and is not safe to run at this table's scale (it's the same shape of query that has crashed or exceeded the memory cap elsewhere on this table). Instead, the watermark is read from the table's own Iceberg `$snapshots` system table (`max(committed_at)`), pure manifest metadata at zero row-scan cost.

Do not copy the join-based pattern onto a large table without checking whether it fits in memory first; it does not scale the way the snapshot-metadata pattern does.

### Other things worth knowing

- **Never delete Iceberg table files directly from the object store**, even mid-development test debris. A `mc rm --recursive` against a `bronze/` S3 prefix once deleted a table's underlying files without dropping it through the catalog first; Nessie's catalog record kept pointing at the now-missing manifest, and every subsequent append failed with `FileNotFoundError`. Always `DROP TABLE` through Trino first. If a dlt load package is left in a partially-loaded state by an unrelated fault like this, resume it with `pipeline.load()` against the same working directory rather than re-running `pipeline.run()` from scratch, which would re-extract and re-append every resource, duplicating tables that already loaded correctly.
- **`make build`'s default target must stay `trino`.** The `duckdb` target in `profiles.yml` is not wired to the Nessie REST catalog and fails on model one; it is dead weight until someone adds an `attach:` block for duckdb's Iceberg extension.
- **`make seed`, `make build`, and `make test` all depend on steps that are easy to forget on a machine that already has state.** A machine with `generation/output/` already populated, or `dbt_packages/` already installed from an earlier manual `dbt deps`, will hide the fact that a genuinely fresh clone needs those steps explicit. If you're debugging "works for me but not in CI or for a new teammate," suspect one of: missing `generation/output/` (need `make generate`), missing `dbt_packages/` (need `make deps`), or a `--target duckdb` left over from habit instead of `--target trino`.
- **MinIO's host API port is not actually independently configurable** despite `.env.example` implying `MINIO_API_PORT` is a free choice: `infra/docker-compose.yml`'s Nessie service hardcodes the container-internal endpoint `http://minio:9000`, and Nessie hands that literal value back to every pyiceberg client regardless of what host port MinIO is mapped to externally. Practical effect: you cannot run two full copies of this stack side by side on one machine unless the first copy's MinIO isn't on host port 9000, or is stopped first. Postgres has the same gap (`"127.0.0.1:5432:5432"` hardcoded, no override var). Neither is fixed; both are documented in `.notes/open-questions.md` for whoever needs concurrent stacks next.
- **elementary's dbt hooks are project-wide, not scoped to `--select`.** Any dbt invocation against the `trino` target, WAP or not, writes elementary's own bookkeeping tables (`dbt_invocations`, `dbt_run_results`, `elementary_test_results`) into the plain `dev` schema. On a Nessie branch, that schema name is not the same table as main's copy, and Nessie correctly detects the divergence as a real merge conflict. `ops/wap.py` passes elementary's own documented `disable_*` vars on every invocation to avoid this; if you're writing a new script that touches a Nessie branch and runs dbt, do the same.
