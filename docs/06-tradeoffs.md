# Tradeoffs

What this project deliberately chose not to do, and why. Every item below maps to a real
decision recorded in `.notes/` (gitignored working log) during the build, not a hypothetical.
Each states the choice made, the alternative considered, and the actual reasoning, not a hedge.

## dim_plan's Type 3 columns are structurally NULL, not derived

`dim_plan` is built full-refresh from `silver_plans`, which carries only current plan state: no
price or tier change history reaches silver, because the source generator does not emit it.
A from-scratch full-refresh build therefore has no prior gold state to diff a new silver row
against. Rather than fabricate a synthetic prior value, `previous_tier`, `previous_price_usd`,
`tier_change_date`, and `price_change_date` are left structurally NULL on every real row (30
real plans in the built table, plus the unknown member). This is documented on the model itself
and on each affected column, so it reads as an intentional, explained state rather than an
unexplained gap.

The alternative was converting `dim_plan` to incremental. Under that design, a plan's Type 3
columns would start populating for real: the model would diff an incoming silver row against
this same model's own previously materialized row for that `plan_id`, and a genuine price or
tier change between two runs would populate `previous_price_usd`/`previous_tier` for the first
time. That conversion was scoped out of the work package that built `dim_plan` and left as
explicitly later work; the dimension is correct and honest in its current full-refresh form, it
simply cannot demonstrate the Type 3 mechanic on data that has never yet changed under it.

## fct_daily_subscription_snapshot denormalizes subscription_status, guarded by a reconciliation test

The 36.5-million-row daily snapshot repeats `subscription_status` directly on the fact rather
than requiring every status filter to join back to `dim_subscriber`. This is a deliberate
denormalization for scan-cheap filtering at this row count: `dim_subscriber` remains the source
of truth, and the fact's copy is only ever as correct as the point-in-time join that produced it.

The guard against that copy drifting from the source is a reconciliation test scoped to the most
recent snapshot day only (roughly the 50,000-row current roster), not all 27+ million historical
rows. Every earlier day's `subscription_status` is, by construction, copied straight from
whatever `dim_subscriber` row the point-in-time join matched at build time, so re-checking every
historical row would mostly restate the model's own join logic rather than catch a real bug. The
most recent day is the one day where the fact's resolved status can be checked against
`dim_subscriber`'s live `is_current` row for genuine drift, which is exactly the class of bug an
interval-join off-by-one would produce. The test uses an `exists()` form rather than scalar
equality specifically to avoid a NULL-versus-no-match ambiguity. The alternative, reconciling
every row on every run, was rejected as expensive and largely redundant with the join logic it
would be re-deriving, not as a shortcut around real risk.

## fct_signup_funnel stayed full-refresh, not incremental

Four of the five facts converted to incremental Iceberg MERGE watermarked on bronze's
`_ingested_at` (see below). `fct_signup_funnel` deliberately did not, even though it is by far
the smallest of the four candidates (about 70,000 rows, a few seconds to fully rebuild).

The reason is not size, it is that this fact's `funnel_status` and `is_completed` columns depend
on wall-clock time relative to `registered_at` (a 30-day expiry window), not only on new source
data. A row can legitimately need to flip from `in_progress` to `expired` between two runs even
when its underlying silver record has not changed at all and its `_ingested_at` has not moved.
A naive incremental design watermarked on `_ingested_at`, the same design used for the other
four facts, would never revisit such a row and would leave it wrongly stuck at `in_progress`
past its true expiry. At 70,000 rows, the full-refresh cost is trivial, so the correctness risk
of a time-dependent incremental design was judged not worth taking on for the size of table
involved. All six dimensions and the bridge table stay full-refresh for a related but distinct
reason: each is small enough (under 210,000 rows, most far smaller) that full-refresh cost is
negligible, and for the two SCD dimensions specifically, incremental SCD maintenance (diffing
incoming rows against the current live version rather than rebuilding history from scratch) is
meaningfully more complex to get right than incremental append-only fact processing, with no
performance justification at this scale to take on that risk.

## Which facts converted to incremental, and the crossover-point reasoning

Four facts converted to incremental Iceberg MERGE, all watermarked on bronze's `_ingested_at`
carried unchanged through silver, deliberately not on the fact's own event-time column: silver
is rebuilt full-refresh every run but preserves each row's original `_ingested_at`, so an
`_ingested_at` watermark correctly identifies rows genuinely new to a gold incremental run
regardless of how old their event time is. A watermark on event time would silently drop this
project's deliberately injected out-of-order arrivals (old event timestamps landing in
later-ingested files), which is exactly the failure mode the pathology exists to demonstrate
catching.

Whether incremental was actually worth its complexity at each table's real size was evaluated
per table, honestly, rather than assumed uniformly correct because the plan called for it
everywhere:

- **fct_watchlist_adds** (~750,000 rows, 3.54s full rebuild): the honest opinion, stated
  plainly in the working log, is no, not really. The steady-state saving is real (roughly 2 to
  2.5 seconds off a run that already finishes in single digits) but small in absolute terms, and
  it costs meaningfully more moving parts (a watermark reconstruction join, backfill var
  plumbing, `on_schema_change='fail'` turning a future silver column change into a hard failure
  instead of a silent absorb). This table converted because the work package specified it, not
  because its own economics called for it; a scheduled full-refresh would have been the simpler,
  equally fast choice at this row count.
- **fct_billing_transactions** (~1.5 million rows, 3 to 5 seconds full CREATE TABLE):
  similarly, full refresh was already fine on pure engineering-cost grounds. Incremental MERGE
  with no new data measured 0.64 to 0.97 seconds versus 3.29 to 5.36 seconds full-refresh, a real
  but not operationally significant difference at this size, bought at the cost of a
  `run_query` pre-query workaround for a genuine Trino MERGE limitation (a correlated subquery
  against the MERGE's own target is rejected outright) and a join-based watermark recovery
  because the grain key has to stand in for a bronze column the gold contract deliberately
  excludes.
- **fct_daily_subscription_snapshot** (27 million rows and growing, 45.12s full-refresh versus
  6 to 17.6s incremental): here incremental earns its cost, but for a narrower reason than "the
  table is big." What tips it is the table's shape, not its current size: a periodic snapshot's
  row count grows by a fixed amount (roughly 50,000 rows) every single day for the life of the
  pipeline, so full-refresh cost is O(total accumulated history) and keeps climbing on every
  future run, while this incremental design's cost is O(reprocess_window_days x roster size) and
  stays flat regardless of how many years of history the table eventually holds. A periodic
  snapshot with a bounded retention window (say, only a trailing 90 days kept) would never cross
  this threshold and a cheap daily full-refresh would stay correct indefinitely; this fact's
  unbounded accumulation is what justifies the added complexity (the bounds CTE, a
  true-lifetime-window versus this-run's-window distinction, reprocess-window and backfill var
  plumbing).
- **fct_playback_events** (~120 million rows, the project's dominant volume): the clearest
  case. A full rebuild is minutes, not seconds, and this project's own single-node Trino
  documents hitting its 1.5GB per-query memory cap on full-width scans of this table. This is
  the fact the work package explicitly named as the one where incremental processing should earn
  its complexity, unlike the smaller facts, and the numbers back that framing.

The general rule the project takes away, stated once rather than re-derived per table:
incremental buys real value once a table's full-rebuild cost is large relative to its per-run
delta, either in wall time or in headroom against this stack's tight per-node memory cap. Below
that crossover, incremental mostly adds surface area without a matching payoff, and full refresh
is the right default until growth, or a genuine trickle-load pattern, says otherwise.

## The duckdb target exists in the dbt project but is not wired up

`transform/lakehouse/profiles.yml` declares two targets, `duckdb` (a fast local/CI target) and
`trino` (the real stack), and defaults to `duckdb`. In practice, duckdb is not viable today.
Running any model against it fails immediately with `Binder Error: Catalog "iceberg" does not
exist`: every source in `models/staging/sources.yml` declares `database: iceberg`, which
resolves correctly against Trino's own catalog named `iceberg`, but means nothing to duckdb
unless something attaches a database literally named `iceberg` to it first. Nothing in the
project does that; `profiles.yml`'s duckdb target has `attach: None`, and no Nessie REST catalog
registration exists for duckdb's Iceberg extension anywhere in the repo.

This was found during a red team review, not assumed correct because it was declared in the
profile. The fix applied was not to make duckdb work, but to stop `make build` from silently
defaulting to a target that has never actually built anything: the Makefile's `build` target now
passes `--target trino` explicitly, the only target every model in this project has actually been
built, tested, and verified against. duckdb remains in `profiles.yml` as an aspirational,
not-yet-implemented fast path. Making it real would mean adding an `attach:` block (or an
on-run-start hook) that ATTACHes Nessie's REST catalog as `iceberg` before any model compiles,
then reverifying that every source resolves and that Iceberg v2 semantics, especially the four
incremental MERGE facts, behave the same as they do under Trino's connector, since dbt-duckdb's
own Iceberg write path has never been exercised in this project at all. Documented honestly here
rather than left to look like a working alternative it is not.

## Nessie chosen over Polaris: real branching over ecosystem-default status

The catalog choice was Nessie over Polaris (and Lakekeeper). Nessie is the only one of the three
with catalog-wide, multi-table git-style branching, which is what makes a real write-audit-publish
workflow possible: a branch can hold changes across several tables at once and be merged or
discarded atomically as a unit. Polaris is the Iceberg ecosystem's post-graduation default
catalog and carries the ecosystem-momentum advantages that come with that status, but its
branching model is per-table snapshot refs only, not a catalog-wide construct, which cannot
express a multi-table write-audit-publish transaction the way Nessie's branches can. Lakekeeper
was the leanest option (a Rust implementation) but offers no branching at all.

What was given up by not choosing Polaris: the broader ecosystem's default-path status,
whatever integration and tooling assumptions accumulate around being the reference
implementation, and a simpler mental model (per-table refs) for anyone who only ever needs
single-table time travel. What was gained: real branching demonstrated end to end in this
project, including a genuine write-audit-publish gate (`ops/wap.py`,
`.github/workflows/wap-gate.yml`) and a rollback story built on Nessie's branch-reset operation
rather than a workaround. That branching model was not free of surprises: a fresh Nessie
repository's genesis commit cannot be merged from in a first WAP run (a confirmed, reproduced
defect at the pinned version, worked around with a bootstrap commit), and Nessie's Iceberg REST
bridge deliberately exposes only the single snapshot matching the current commit per table,
which broke the naive assumption that Iceberg-native `SELECT ... FOR VERSION AS OF` would work
unmodified on this catalog (it does not; the real mechanism is a checked reference,
`<branch>@<commit-hash>`, passed to Nessie's REST catalog URL, confirmed live and documented in
`docs/evidence/time-travel/`). Both were root-caused, not worked around blindly, and both are a
direct consequence of choosing the catalog that actually delivers the branching this project
wanted.

## Two docker-compose port-parameterization gaps

Found during a reproducibility check (cloning the repo fresh and bringing the stack up under a
different `COMPOSE_PROJECT_NAME` to prove it works from nothing), not fixed, because fixing them
was outside that pass's mandate absent a concrete forcing need; the actual reproducibility check
did not strictly need two stacks running concurrently, and stopping the real stack first was
sufficient and safer.

1. **MinIO's host API port is not really independently configurable**, despite
   `.env.example`/`docker-compose.yml` implying otherwise via `MINIO_API_PORT`. The Nessie
   service hardcodes `nessie.catalog.service.s3.default-options.endpoint:
   http://minio:9000` (the container-internal address, always port 9000 regardless of the host
   port mapping), Nessie hands that literal value back to every pyiceberg client, and the
   ingestion layer's host-side hostname alias (`ingestion/network.py`, which resolves `minio`
   and `nessie` to `127.0.0.1` for host-run pipelines) can only fix up a hostname, not a port
   number. Practical effect: `make seed` only works if MinIO's host port is actually 9000;
   changing `MINIO_API_PORT` breaks ingestion silently for anyone who assumes the variable is a
   free choice, and two full copies of this stack cannot run side by side on one machine under
   different project names unless the first copy's MinIO is not on host port 9000. The real fix
   would be parameterizing the Nessie service's endpoint config from `${MINIO_API_PORT:-9000}`
   in `docker-compose.yml`, a small, mechanical, low-risk change, left for whoever next actually
   needs two concurrent stacks.
2. **Postgres has no port override at all.** Unlike MinIO, Nessie, and Trino, which all support
   an environment variable override, the postgres service hardcodes
   `"127.0.0.1:5432:5432"`. This is the same class of gap as the MinIO finding: even after fixing
   MinIO's endpoint parameterization, a second concurrent stack would still collide on Postgres's
   host port. The fix would follow the existing pattern exactly (`${POSTGRES_PORT:-5432}`), left
   unfixed for the same reason: no concrete need forced it during this pass, which worked around
   the gap by stopping the real stack first.

## Nessie has no automatic retention; the manual nessie-gc policy is documented, not scheduled

Iceberg's own `expire_snapshots` procedure has nothing meaningful to expire on this catalog,
because each table only ever has one live snapshot visible through Nessie's REST bridge (the
same deliberate, documented single-snapshot design behind the time-travel tradeoff above).
Nessie itself never auto-expires anything, neither commits nor the underlying data files they
reference. The real retention tool is a separate program, `nessie-gc`, which does mark-and-sweep
of orphaned data files against a per-reference cutoff policy. It is not part of this stack's
`docker-compose.yml` today, and running it is a manual or scheduled operation that does not
currently exist anywhere in this project.

A retention policy was chosen and justified rather than left as an open question:
`main=P14D`, `wap_.*=P3D`, `time_travel_demo=P7D`, and a `default-cutoff=P30D` backstop for
anything unmatched, all time-based rather than commit-count-based given this project's uneven,
bursty commit cadence (several bookkeeping commits can land from a single dbt run). The
reasoning per reference class is written out in
`docs/evidence/time-travel/09-retention-policy-notes.txt`. What was deliberately not done: this
policy was never run against the live stack. It is a documented, justified policy and the literal
command that would enforce it, not an executed action, which matches the scope boundary of the
work that produced it. Storage growth on this catalog is therefore currently unbounded in
practice; the policy exists on paper as the answer, not yet as a running job.

## Iceberg v2 chosen over v3

The table spec is pinned to Iceberg v2, not v3, across the whole project. v3 (deletion vectors,
row lineage) shipped in core Iceberg 1.11, but Trino and PyIceberg, the two engines this stack
actually depends on for reads and writes, do not fully support it yet. v2 is the version where
Trino, dbt-trino, PyIceberg, and DuckDB all agree, which matters because this project deliberately
runs the same tables through multiple engines (Trino for the real warehouse, PyIceberg inside the
ingestion pipeline, DuckDB as the aspirational fast-path target). Choosing v3 would have meant
picking a spec version at least one engine in that chain cannot fully honor. v3 was noted at the
time as a possible stretch or DuckDB-only exploration if time allowed, explicitly not a
commitment, and nothing in the build since has changed that calculus: the multi-engine
compatibility gap that ruled it out at the start is still the same gap today.
