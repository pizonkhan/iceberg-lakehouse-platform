# MERGE semantics and idempotency

Four fact tables in this project write through Iceberg `MERGE` instead of a full `CREATE TABLE
AS`: `fct_playback_events`, `fct_billing_transactions`, `fct_watchlist_adds`, and
`fct_daily_subscription_snapshot`. Getting a MERGE-based incremental load right means two separate
things have to hold at once: the statement itself must be semantically sound (an `ON` clause that
can only ever match one target row per source row), and repeated runs against unchanged input must
converge to the same content, not silently drift. This document works through both, using this
project's own model code and its own measured checksums as evidence rather than asserting either
property in the abstract.

Column-level source of truth: `.notes/modeling.md`, "Incremental processing: which facts convert,
which stay full-refresh". Real model code: `transform/lakehouse/models/marts/facts/
fct_billing_transactions.sql`, `fct_watchlist_adds.sql`, `fct_daily_subscription_snapshot.sql`,
`fct_playback_events.sql`.

## The problem it solves, stated precisely

A full-refresh model (`materialized='table'`) answers "what should this table contain" by
recomputing the whole answer from scratch every run. That is simple and, for a small table, cheap.
It stops being cheap once a table's total size grows much larger than the volume of genuinely new
data arriving per run: `fct_playback_events` at ~120M rows costs 242.68 seconds to rebuild from
scratch on this project's Trino, while a run that only has to absorb the delta since the last run
costs 11 to 21 seconds. Incremental `MERGE` exists to close that gap: instead of recomputing the
whole table, a run computes only the rows that are new or changed since the last run, and asks the
warehouse to reconcile that small "source" set against the existing "target" table in one
statement.

The cost of that efficiency is correctness risk that a full rebuild never has to think about. A
full refresh cannot desynchronize from its own source, because it has no memory of a prior run at
all; every run is definitionally correct relative to whatever silver contains right now. A MERGE,
by contrast, depends on getting three things right every single run: the `ON` condition must
identify at most one target row per source row (or the statement is not well-defined and the
engine has to refuse it), the watermark that decides "what counts as new" must never skip a row
that is genuinely new, and repeated runs against the same input must not accumulate drift. This
document is about how this project's four incremental facts satisfy all three.

## The mechanism, from first principles

SQL:2016's `MERGE` statement (Iceberg's `MERGE INTO` follows the same shape) reconciles a target
table against a source set using an `ON` predicate, and dispatches each source/target pairing into
one of three branches:

- **`WHEN MATCHED`**: the `ON` predicate found a target row for this source row. The branch
  typically runs an `UPDATE`, overwriting the target row's columns with values derived from the
  source row.
- **`WHEN NOT MATCHED [BY TARGET]`**: a source row exists with no corresponding target row. The
  branch typically runs an `INSERT`, adding a new row.
- **`WHEN NOT MATCHED BY SOURCE`**: a target row exists with no corresponding source row. The
  branch, if present, typically runs a `DELETE` or an `UPDATE` that marks the row stale.

Every incremental fact in this project uses only the first two branches. None of them declares a
`WHEN NOT MATCHED BY SOURCE` clause, and that absence is deliberate, not an oversight: this
project's incremental design is watermark-scoped (a run's source set is "rows new since the last
watermark," never "the full current state of silver"), so a target row having no counterpart in
one run's narrow source set is the expected, permanent condition for every row that isn't part of
this run's delta, not a signal that the row should be deleted. dbt's `incremental_strategy='merge'`
compiles exactly this shape: matched rows are updated in place by their `unique_key`, unmatched
source rows are inserted, and nothing in the target is ever removed by the statement itself. Every
one of this project's four incremental facts declares this config identically in its top-of-file
block, differing only in the `unique_key`:

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='billing_transaction_id',
        on_schema_change='fail'
    )
}}
```

`fct_daily_subscription_snapshot` is the one exception in shape, not in kind: because its grain is
a composite of two columns, its `unique_key` is a list rather than a single column:

```sql
unique_key=['snapshot_date_key', 'subscriber_sk'],
```

The `ON` predicate dbt-trino compiles from `unique_key` is equality across every listed column,
joined with `AND`. That single design choice, which columns make up `unique_key`, is what the next
section is actually about.

## The multiple-match error class

`MERGE`'s `ON` predicate is only well-defined if it identifies **at most one** target row for each
source row, and at most one source row for each target row. If the join that predicate describes
would, for some key, match a target row against two or more source rows (or a source row against
two or more target rows), the statement has no unambiguous instruction to execute: an `UPDATE`
branch cannot apply two different sets of column values to the same physical row in one statement.
This is not an engine-specific quirk; it is baked into the SQL standard's own definition of
`MERGE`, and every engine that implements the standard, Trino included, refuses to execute a
statement whose join fans out this way rather than picking one of the ambiguous matches silently.
Trino surfaces this as a distinct error class (`MERGE_TARGET_ROW_MULTIPLE_MATCHES`, "one MERGE
target table row matched more than one source row"), separate from the correlated-subquery error
this project's builders actually did hit and is covered below.

None of this project's four incremental facts has ever needed to reproduce that error live,
because each one's `unique_key` was chosen, up front, to be the fact's actual declared grain key,
not a convenient-looking column that happens to look unique. Modeling.md states the grain for each
fact and the `unique_key` is a direct restatement of it:

- `fct_playback_events`: "one row per completed playback session ... `playback_session_id` is the
  degenerate dimension and the unique, incremental merge key." A session can only be one physical
  event; there is no source-side transformation upstream of this model that could legitimately
  produce two rows sharing one `playback_session_id`.
- `fct_billing_transactions`: "one row per discrete billing ledger event ... `billing_transaction_id`
  is the degenerate dimension and unique merge key." `silver_billing_ledger` is explicitly
  deduplicated on this id before this model ever reads it (row_number() partitioned by
  `billing_transaction_id`, ordered `_ingested_at desc, _batch_id asc`, keep rank 1), which is what
  makes the real count 1,500,100 rows to 1,500,100 distinct ids, not the 1,515,049 raw bronze rows
  that include the pathology-5 duplicate-key injections.
- `fct_watchlist_adds`: "one row per event of one subscriber adding one title to their watchlist
  ... `watchlist_event_id` is the unique merge key." Modeling.md is explicit about the trap a
  different key choice would walk into: "a re-add after a removal is a new event row, so
  (subscriber, title) pairs can repeat with distinct timestamps." Had this fact's `unique_key` been
  `(subscriber_sk, title_sk)` instead of the true event id, a subscriber re-adding the same title
  twice would produce exactly the multiple-match shape described above: two genuinely distinct
  source rows sharing one candidate target key, and `MERGE` would refuse to run rather than pick
  one arbitrarily.
- `fct_daily_subscription_snapshot`: "Unique key: `(snapshot_date_key, subscriber_sk)`; a
  subscriber has exactly one version current at any end-of-day instant, so this composite is
  unique." The composite is required here specifically because neither column alone identifies a
  row: the same `subscriber_sk` recurs across every day of that subscriber's tenure, and the same
  `snapshot_date_key` recurs across every subscriber.

The mechanical guarantee this buys is: `unique_key` must be unique **within the source set a given
run presents**, not unique across all of history in the abstract. A source row set that is itself
free of duplicate keys, joined against a target that `MERGE`'s own prior runs have already kept
free of duplicate keys (each prior run only ever inserted or updated by that same key), can never
produce a multiple match, by induction on runs. The one honest caveat to this argument, not glossed
over: `silver_playback_sessions` is the one silver table in this project that is **not**
positively verified duplicate-free on `playback_session_id`. A `COUNT(DISTINCT playback_session_id)`
over ~120M rows exceeds this project's single-node Trino's 1.5GB query memory cap
(`EXCEEDED_LOCAL_MEMORY_LIMIT`, confirmed directly), so the real check that was run instead is
`approx_distinct()`: 119,645,576 against a true row count of 120,000,300, a gap fully inside
HyperLogLog's expected error margin at this cardinality. That is evidence of no duplication, not
proof of it; it is recorded plainly rather than silently treated as equivalent to the exact checks
run on the other three facts' merge keys.

## Proof sketch: this project's incremental models are idempotent

"Idempotent" here means a specific, checkable claim: running the same `MERGE` again, with the
identical source rows as the previous run, against a target that already reflects those rows,
leaves the target's business content unchanged. Two different scenarios both have to hold for that
claim to be true, and this project has real evidence for both, not just the trivial one.

**Scenario one: the source set is empty.** If the watermark filter admits zero new rows (the
steady-state case, no new bronze ingestion since the last run), the `MATCHED` and `NOT MATCHED`
branches never execute at all: there is nothing to match against and nothing to insert. This is
the cheapest possible form of idempotency, and it is what this project's "three consecutive runs"
checks primarily exercise. It proves the watermark logic is sound (a stable watermark keeps
re-excluding the same already-loaded rows), but it does not, by itself, prove the `UPDATE` branch
is actually idempotent, since that branch never runs in this scenario.

**Scenario two: the source set is non-empty and every row matches.** This is the stronger case,
and it is the one that actually tests whether the `MATCHED` branch's `UPDATE` is a pure function of
its inputs. If a run's source rows are all already present in the target (a backfill re-covering
an already-loaded range, or the rolling reprocess window `fct_daily_subscription_snapshot` runs
every time), the `MERGE` genuinely rewrites every matched row's non-key columns. For that rewrite
to be idempotent, every column in the `UPDATE`'s output expression, other than `loaded_at`, has to
be a deterministic function of columns that have not themselves changed: the same silver rows, the
same dimension state. `loaded_at` is excluded from every idempotency comparison in this project on
exactly this basis, per modeling.md's own naming convention: "Audit: every gold table carries
`loaded_at` TIMESTAMP(6) NOT NULL, the wall-clock time the gold merge wrote the row. Never used in
any join or hash." Every other column, by construction, has no wall-clock or random component.

### Real evidence, both scenarios, reproduced from `.notes/decisions.md`

`fct_billing_transactions`, no-op scenario: baseline full-refresh built 1,500,100 rows. Three
consecutive incremental `dbt build` runs against unchanged upstream data each logged
`MERGE (0 rows)`, and an order-independent content checksum over every non-`loaded_at` column
(Trino's `checksum()` aggregate) was byte-identical across the full-refresh baseline and all three
incremental runs: `f6 07 6b e6 3e 79 d1 df`.

`fct_billing_transactions`, forced full-rewrite scenario, the stronger proof: a later red-team pass
used this fact's own documented backfill mechanism with a window covering the entire dataset, which
"makes the MERGE re-match and rewrite all 1,500,100 existing rows (a real, substantial write, not a
trivial no-op merge) while being provably content-neutral." Baseline checksum before the exercise:
`BA98E50C4EF99C85`. After deliberately killing one live MERGE mid-write (see the recovery discussion
below) and rerunning the identical backfill command to completion twice: "the table's final row
count and checksum matched the original baseline exactly after each." A `MERGE` that genuinely
rewrote 1,500,100 rows twice in a row, from real, matched, `UPDATE`-branch execution rather than a
skipped no-op, still converged on the same checksum. This is the direct proof that the `UPDATE`
branch's output expressions are pure functions of unchanged inputs, not the weaker no-op-only proof.

`fct_watchlist_adds`, no-op scenario: full-refresh baseline 750,000 rows, checksum `C3B540624D04AE72`.
Three consecutive incremental runs against unchanged data: `MERGE (0 rows)` each time, checksum
unchanged (`C3B540624D04AE72`) across all three. A backfill covering the table's one real
`_ingested_at` value forced a genuine rewrite scenario here too: "ran as MERGE (750,000 rows), all
matched and updated in place ... the non-loaded_at checksum still C3B540624D04AE72 (business
content unchanged), and loaded_at collapsed to a single new timestamp identical across all rows
(proof the merge actually rewrote every row rather than leaving them untouched)."

`fct_playback_events`, no-op scenario, at this project's largest scale: three consecutive
incremental runs against unchanged upstream data ran in 20.78s, 12.41s, and 11.30s, each reporting
identical row count (119,640,099) both by exact count and by Iceberg's own snapshot history ("no
new snapshot was committed by any of the three no-op MERGEs ... Trino's Iceberg connector does not
even write an empty commit when a MERGE's source matches zero rows"). Because a full-width content
checksum is not viable at this table's physical scale (covered below), the checksum methodology
here is a genuine, documented reduction in scope: a 30-day evenly-spread sample across the full
2023-01-01 to 2026-08-03 range, 3,288,953 sampled rows (2.7% of the table), XORed across 30 partial
`xxhash64` checksums. That combined value, `-8261973429135120039`, was identical across all three
incremental runs, "the actual idempotency evidence, alongside the full, exact whole-table row count
(also identical all three times)."

`fct_daily_subscription_snapshot` is the one fact where the rewrite scenario is not an occasional
backfill exercise but the model's own default, every single run: its `reprocess_window_days`
design re-generates and re-merges the most recent 3 days of already-written history on every
incremental run, on purpose, to absorb late corrections. Three consecutive incremental runs with no
new day eligible produced byte-identical row count (27,011,346) and content checksum
(`725378a3d48c791d`) against the full-refresh baseline, while a direct check of which rows each run
actually touched (`loaded_at` newer than the run's start) confirmed "exactly 149,384 rows each
time, spanning snapshot_date_key 20260801 through 20260803, i.e. precisely the 3-day reprocess
window and nothing else." That is 149,384 genuinely re-matched, re-`UPDATE`d rows on every run,
converging to the same checksum every time, the same structural proof as billing's red-team
exercise, just happening by design on every ordinary run instead of only under deliberate testing.

## The real dbt-trino limitation: a correlated subquery that isn't supposed to be one

The naive way to compute an ingestion-time watermark is a scalar subquery inline in the model's
`WHERE` clause:

```sql
where _ingested_at > (select max(_ingested_at) from {{ this }})
```

Every one of the three facts that tried this first hit the identical wall against this project's
real Trino. The exact error, quoted directly from `.notes/decisions.md`:

> `TrinoUserError(NOT_SUPPORTED, "Given correlated subquery is not supported")`

The root cause is a compilation detail of dbt-trino's `merge` incremental strategy, not a flaw in
the SQL as written. dbt-trino compiles the model's own `is_incremental()`-filtered `SELECT` into a
view that becomes the `MERGE` statement's `USING` source. The subquery above, which looks
uncorrelated (it reads `{{ this }}`, the eventual MERGE target, from inside what becomes the
MERGE's own source), ends up compiled as:

```sql
MERGE INTO target USING (
    SELECT ... WHERE x > (SELECT max(x) FROM target)
) AS src ON ...
```

a scalar subquery against the `MERGE` statement's own target table, sitting inside that same
statement's source. Trino's planner rejects that shape outright. A second, subtler variant of the
same trap surfaced independently on `fct_watchlist_adds`: because the gold fact table's column
contract (six columns, per modeling.md) does not include `_ingested_at` at all, the naive subquery
does not fail with a column-not-found error; Trino's planner instead silently resolves the
unqualified `_ingested_at` name against the outer query's silver alias, which turns an apparently
uncorrelated aggregate subquery into a genuinely correlated one, and hits the identical
`NOT_SUPPORTED` error for a different structural reason than the billing case. Both were confirmed
directly against this project's real Trino before any fix was written, not inferred from
documentation.

## Fix one: join-based watermark recovery (billing, watchlist)

`fct_billing_transactions` and `fct_watchlist_adds` both fix this the same way: resolve the
watermark to a plain literal timestamp via a `run_query` pre-query, guarded by `{% if execute %}`,
executed before the model's main `SELECT` is assembled. This sidesteps the correlated-subquery
error entirely, because the compiled `MERGE`'s `USING` source never contains a subquery against the
target at all, just a literal value substituted in ahead of time. Because the gold contract for
both facts excludes bronze/silver metadata columns, the pre-query cannot read `_ingested_at`
directly off `{{ this }}` either; it reconstructs each already-merged row's original `_ingested_at`
by joining the target back to silver on the fact's own grain key. From
`fct_billing_transactions.sql`, quoted directly:

```sql
{% set get_max_ingested_at_query %}
    select coalesce(max(sbl._ingested_at), timestamp '1900-01-01 00:00:00.000000') as max_ingested_at
    from {{ ref('silver_billing_ledger') }} as sbl
    inner join {{ this }} as f
        on f.billing_transaction_id = sbl.billing_transaction_id
{% endset %}
{% if execute %}
    {% set max_ingested_at_value = run_query(get_max_ingested_at_query).columns['max_ingested_at'].values()[0] %}
    {% set max_ingested_at_literal = max_ingested_at_value.strftime('%Y-%m-%d %H:%M:%S.%f') %}
{% endif %}
```

and the identical pattern in `fct_watchlist_adds.sql`:

```sql
watermark as (
    select coalesce(max(s._ingested_at), timestamp '1900-01-01 00:00:00.000000') as watermark_at
    from {{ this }} as f
    inner join {{ ref('silver_watchlist_adds') }} as s
        on f.watchlist_event_id = s.watchlist_event_id
),
```

This is correct because silver's `_ingested_at` for a given grain key never changes across
silver's own full rebuilds (it is bronze metadata passed through verbatim, never recomputed), so
the maximum `_ingested_at` among rows matched by the join is exactly the ingestion watermark of the
last successfully merged batch. It works within the model's fixed column contract, at the cost of a
join between the target and silver on every run.

## Fix two: reading the watermark from Iceberg's own snapshot metadata (playback)

`fct_playback_events` cannot reuse the join-based fix, and the reason is quantitative, not
stylistic. A join between `{{ this }}` and silver on `playback_session_id`, at this fact's ~120M-row
scale, is structurally the same operation this project has already confirmed twice exceeds this
Trino instance's 1.5GB-per-node memory cap: an exact `COUNT(DISTINCT playback_session_id)` over the
same table, and this same model's own backfill-window memory testing (a full-range backfill window
"reliably hit `EXCEEDED_LOCAL_MEMORY_LIMIT` across three separate clean-state attempts, with
`HashBuilderOperator` (450MB, 660MB, 763MB across the three attempts, growing each time)"). Recovering
a scalar watermark by paying for a 120M-row hash join on every single incremental run, including the
steady-state no-op case, would make the watermark computation itself the most expensive part of the
run, defeating the entire point of converting the fact to incremental in the first place.

The fix instead reads the watermark from a different source entirely: Iceberg's own snapshot
history, exposed by Trino as a system table suffixed `$snapshots`. From `fct_playback_events.sql`:

```sql
{% set snapshots_relation = this.incorporate(path={"identifier": this.identifier ~ "$snapshots"}) %}

...

where _ingested_at > (
    select coalesce(max(committed_at), timestamp '1900-01-01 00:00:00.000000 UTC')
    from {{ snapshots_relation }}
)
```

`"fct_playback_events$snapshots"` is a distinct relation from `{{ this }}` itself, so this is not a
self-reference and does not trip the correlated-subquery limitation that motivated fix one in the
first place: dbt's own compiled output for this model was checked directly, confirming the
generated `MERGE ... USING` statement's source references the `$snapshots` relation, never the
target table. `committed_at` on that system table is Iceberg manifest metadata: the wall-clock time
each snapshot (each write this model has ever made) was committed. `max(committed_at)` answers "when
was this table last written," which stands in for "which bronze `_ingested_at` values are already
incorporated," under the same monotonicity assumption every processing-time watermark relies on:
`_ingested_at` is assigned by ingestion's own clock, so a later gold commit is never earlier than
the `_ingested_at` of bronze data already merged into it as of that commit.

### Why this is the more elegant solution, not just a differently-shaped workaround

The join-based fix and the snapshot-metadata fix solve the identical correlated-subquery problem,
but they are not equally good solutions, and the difference is architectural, not cosmetic:

- **Cost shape.** The join-based fix costs `O(rows already in the target)` on every single run,
  win or lose, because it has to scan and join the full target and silver tables to reconstruct a
  value. The snapshot-metadata fix costs `O(number of snapshots)`, which for this table is one
  commit per successful run, not one row per fact row. At 750k or 1.5M rows the difference is
  immaterial; at 120M rows it is the difference between a query that fits comfortably and one that
  crashes the coordinator.
- **What it depends on.** The join-based fix depends on a specific relationship being true forever:
  that this fact's grain key is a column that also exists on the silver table it needs to join
  against, so `_ingested_at` can be recovered through it. That happens to hold for all three
  smaller facts because their grain keys are literally the silver ledger's own ids, but it is an
  assumption, not a structural guarantee, and a future fact whose grain key does not appear on its
  own silver source would need a different join entirely. The snapshot-metadata fix depends on
  nothing about the fact's own column shape: it reads commit metadata that Iceberg maintains for
  every table automatically, regardless of what columns that table happens to carry.
- **What it touches.** The join-based fix reads business data (silver rows, target rows) to derive
  what is fundamentally a piece of load bookkeeping. The snapshot-metadata fix reads load
  bookkeeping to derive load bookkeeping: it never scans a single business-data row to compute the
  watermark. That is the more architecturally honest shape for what a watermark actually is.

The choice was made deliberately, against the grain of what the initial work package's own
instructions suggested (a `run_query` pre-query reading `max(_ingested_at)` from the target, the
same join-based approach the two sibling facts used). The engineering judgment recorded in
`.notes/decisions.md` states the tradeoff explicitly: the join-based pattern is "safe at their row
counts (1.5M and 750k), is not safe to copy onto this table without modification." The
snapshot-metadata read is not a bigger-scale variant of the same trick; it is a genuinely different
mechanism that happens to also sidestep the correlated-subquery limitation as a side effect, while
scaling independently of the table's own row count in a way the join-based fix structurally cannot.

## When it fails, and the counter-indications

**A `unique_key` that does not match the fact's true grain reintroduces the multiple-match
problem.** This is not hypothetical: modeling.md documents the exact trap for
`fct_watchlist_adds` directly (re-adds after a removal produce repeat (subscriber, title) pairs),
and the fact's real `unique_key` is `watchlist_event_id` specifically to avoid it. Anyone extending
this model to key on a coarser combination would reproduce the multiple-match failure class the
moment the source data contains a real re-add.

**The snapshot-metadata watermark assumes the catalog actually preserves commit history.** This
project's own red-team pass found a real surprise about Nessie specifically: "Nessie's REST catalog
does not appear to preserve Iceberg-native snapshot lineage across commits the way a plain
Hadoop/Glue catalog would ... every single commit writes a fresh `00000-<uuid>.metadata.json`
(version-number 0), never `00001-`, `00002-`, etc. building on the last one." This did not break
`fct_playback_events`' watermark design, because that design only ever needs the single most recent
commit's `committed_at`, never the full history, and the current snapshot's own row in `$snapshots`
was always present and correct in every test run. But it is worth stating plainly as a boundary
condition: a design that needed to read further back than the latest snapshot (for example, "what
was the watermark two runs ago") would run straight into this catalog-specific limitation, where
the join-based fix, reading ordinary row data rather than catalog metadata, would not.

**A MERGE still needs real memory for the match itself, independent of scan cost.** Even with the
watermark resolved cheaply, the `MERGE` statement's own execution plan builds a hash table to match
source rows against target rows on the unique key, on top of whatever the underlying scan already
costs. This is a real, separate cost from the scan, confirmed directly on `fct_playback_events`: a
backfill window wide enough to match a large fraction of the table's ~120M rows hit
`EXCEEDED_LOCAL_MEMORY_LIMIT` even though the equivalent full-refresh `CREATE TABLE AS` over the
same row count succeeds, because the CTAS never has to build that matching hash table at all. This
is a genuine structural finding, not flakiness, and it is the reason this fact's backfill mechanism
carries an explicit sizing warning in its own header comment rather than being presented as safe at
any window width.

**A killed MERGE never leaves the table in a partial state, and that is worth demonstrating rather
than assuming.** This project's red-team pass deliberately killed a real, live Trino MERGE query
mid-write against `fct_billing_transactions`, once during the scan/plan phase and once after a real
Parquet file had already been written to storage. In both cases the killed query surfaced as
`state=FAILED, error_type=USER_ERROR, error_code=ADMINISTRATIVELY_KILLED`, the table's row count and
content checksum were unchanged immediately after, and a `SELECT count(*)` succeeded throughout with
no locked or unavailable window. The orphaned Parquet file from the second kill was confirmed
physically present in MinIO but referenced by zero rows in `"fct_billing_transactions$files"`,
meaning the committed snapshot's manifests never pointed at it: Iceberg's atomic commit protocol
means a failed write simply never becomes visible, rather than becoming visible in a corrupted
half-state. Recovery required nothing beyond rerunning the identical, already-idempotent MERGE
command; this is the direct, load-bearing connection between MERGE's transactional guarantee and
the idempotency property this document is about; the property only matters operationally because
rerunning after any failure, whether a crash, a kill, or a routine retry, is always safe by
construction.

## How to verify this is actually working from this repo

**Data tests.** `on_schema_change='fail'` on every incremental fact means an unexpected column
change surfaces as a hard build failure rather than a silent absorb. Run `dbt build --select
fct_billing_transactions fct_watchlist_adds fct_daily_subscription_snapshot fct_playback_events
--target trino` (or `make build`) and confirm each model reports `MERGE (N rows)`, not `CREATE
TABLE`, on a second run against unchanged upstream data.

**Reproduce the no-op checksum proof directly.** After two consecutive `dbt build` runs with no
upstream change, compare row counts and a `checksum()` over every non-`loaded_at` column, matching
the technique this project already used:

```sql
select
    count(*) as row_count,
    to_hex(checksum(md5(to_utf8(
        billing_transaction_id || subscriber_sk || plan_sk || payment_method_sk
        || cast(billing_date_key as varchar) || cast(transaction_posted_at as varchar)
        || transaction_type || cast(amount_usd as varchar) || cast(tax_amount_usd as varchar)
    )))) as content_checksum
from iceberg.dev_facts.fct_billing_transactions;
```

A second run producing the identical `content_checksum` is the same proof this document reproduces
above from `.notes/decisions.md`, run fresh against the live warehouse rather than taken on faith.

**Inspect the snapshot history directly, for the playback fact specifically.** `select * from
iceberg.dev_facts."fct_playback_events$snapshots" order by committed_at desc` shows exactly what
the model's own watermark query reads; confirming a no-op incremental run adds no new row to this
system table (Trino's Iceberg connector does not write an empty commit for a zero-match MERGE) is a
direct, live check of the "no new snapshot on a no-op run" claim made above.

**Force the rewrite scenario deliberately**, rather than relying only on the no-op case, using each
model's own documented backfill vars (quoted in each model's header comment), for example:

```
dbt build --select fct_billing_transactions --target trino --vars \
  '{"backfill_start": "2026-08-04 00:00:00.000000", "backfill_end": "2026-08-05 00:00:00.000000"}'
```

against a range that covers rows already in the table, then re-run the same checksum query. An
unchanged checksum after a run that logged `MERGE (N rows)` with `N` equal to the full table's row
count, not `MERGE (0 rows)`, is the strong idempotency proof: genuine `UPDATE`-branch execution
converging on the same content, not a watermark simply excluding everything.

**Integration tests.** `tests/integration/` runs against the live stack and includes the
uniqueness, relationship, and sign-convention checks referenced throughout this document; `pytest
tests/integration` after a build exercises the same assertions this document quotes evidence for,
against whatever the repo's current build actually produced, not a frozen snapshot of a past run.
