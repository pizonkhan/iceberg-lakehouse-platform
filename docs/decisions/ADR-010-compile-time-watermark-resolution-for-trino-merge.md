# ADR-010: Resolve the incremental watermark at compile time, not as an inline correlated subquery

## Status

Accepted, 2026-08-04.

## Context

The naive way to write an incremental filter in dbt is inline in the model SQL:
`where _ingested_at > (select max(_ingested_at) from {{ this }})`. Three separate incremental fact
conversions in this project (`fct_watchlist_adds`, `fct_billing_transactions`, `fct_playback_events`,
each built by different agents in the same work window) independently hit the same failure trying
this: `TrinoUserError(NOT_SUPPORTED, "Given correlated subquery is not supported")`.

The root cause is dbt-trino's own merge strategy: it compiles the incremental filter into a view
that becomes the MERGE statement's `USING` source, so the naive form's compiled SQL is literally
`MERGE INTO target USING (SELECT ... WHERE x > (SELECT max(x) FROM target)) AS src ON ...`, a
scalar subquery against the MERGE's own target table, inside its own source. Trino's planner
rejects that shape outright.

A second problem followed for two of the three facts: the watermark column (`_ingested_at`) is
bronze and silver bookkeeping, deliberately excluded from every gold fact's column contract, so even
a fixed, non-correlated version of the subquery failed with `COLUMN_NOT_FOUND` against the actual
target table.

## Decision

Resolve the watermark to a plain literal before the main MERGE SQL is assembled, via a dbt
`run_query` pre-query guarded by `{% if execute %}`, so the compiled MERGE source never references
the target table at all. This is the standard workaround for this class of Trino limitation and was
established by the first conversion to hit it, then reused as-is by the other two rather than each
rediscovering it independently.

Because the watermark column does not exist on the gold fact, the pre-query recovers it by joining
silver back to the target on the shared grain key instead of reading a column that was never
written to gold, for example:
`select coalesce(max(sbl._ingested_at), timestamp '1900-01-01 ...') from silver_billing_ledger sbl
inner join {{ this }} f on f.billing_transaction_id = sbl.billing_transaction_id`. Because silver
always carries every row's true original `_ingested_at`, and MERGE only ever adds or updates rows
matched by the grain key, the maximum `_ingested_at` among matched rows is exactly the ingestion
watermark of the last successfully merged batch, with no need for a hidden extra column on the fact.

`fct_playback_events` (~120M rows) deviated from this join-based recovery specifically: a full
`{{ this }}`-to-silver self-join at that row count is the same shape of query this project had
already twice confirmed crashes or exceeds Trino's 1.5GB-per-node memory cap. It instead reads the
watermark from the table's own Iceberg snapshot metadata,
`select max(committed_at) from "fct_playback_events$snapshots"`, a genuinely distinct relation from
`{{ this }}` (no self-reference), at zero row-scan cost regardless of table size.

## Alternatives Considered

- **Persist the watermark column on the gold fact contract.** Would have sidestepped both problems
  trivially: a plain `max(_ingested_at) from {{ this }}` pre-query needs no join at all once the
  column exists on the target. Rejected because it violates the actual constraint driving the
  original design: this work package's mandate was "materialization changes, not the column
  contract," and `modeling.md`'s gold column list is treated as a binding interface, not something a
  materialization change gets to amend.
- **Copy the join-based watermark recovery onto `fct_playback_events` unmodified**, matching the
  other two conversions exactly. Rejected after direct testing: a full self-join at ~120M rows is
  exactly the shape of query already shown to crash or exceed this Trino instance's memory cap
  (the abandoned `unique` test on this same table, and this work package's own checksum
  verification), which would have made the watermark resolution itself the most expensive part of
  every incremental run, defeating the purpose of converting the table at all.
- **A dedicated watermark tracking table**, written by each incremental model as a side effect,
  read back on the next run instead of derived from silver or Iceberg metadata. Not pursued: it
  would add a new piece of state to keep consistent with the fact table itself (what happens if the
  MERGE succeeds but the watermark write does not, or vice versa) for a problem the snapshot-metadata
  and join-based approaches both already solve without introducing new state.

## Consequences

- Three different watermark-resolution mechanisms now exist across four converted facts (a
  join-based recovery for two, Iceberg snapshot metadata for the largest one), not a single reusable
  pattern. A future incremental conversion cannot assume either approach is safe without checking
  its own table's scale against the same memory ceiling that ruled out the join for
  `fct_playback_events`.
- The `run_query`-at-compile-time pattern itself (resolve to a literal before the MERGE source is
  built) is sound and portable across both variants; only the second half (how the literal is
  computed) differs by table size.
- dbt-trino's incremental materialization does not clean up its own intermediate `__dbt_tmp` relation
  when a run is killed mid-flight, which surfaced repeatedly while iterating on this pattern under
  memory pressure: a dangling `__dbt_tmp` relation from one failed attempt collides with the next
  attempt's own CREATE and has to be dropped by hand through the catalog before retrying.
