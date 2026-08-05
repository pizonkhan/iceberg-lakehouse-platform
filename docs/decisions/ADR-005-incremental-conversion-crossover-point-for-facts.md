# ADR-005: Which facts converted to incremental MERGE, and the crossover point behind the split

## Status

Accepted, 2026-08-04.

## Context

Every fact and dimension in this project started as a full-refresh table, built correctness-first
per an explicit precedent (`dim_plan`, `dim_title`, `dim_subscriber` were all built full-refresh on
the stated basis that incrementality is a later concern). A later work package converted a subset of
facts to incremental Iceberg MERGE and left the rest, including every dimension, as full-refresh.

Each conversion recorded real, measured numbers, not estimates, and each conversion's own entry
states an honest opinion on whether the complexity was worth it at that table's actual size:

- `fct_watchlist_adds` (750,000 rows): full-refresh CTAS 3.54s; steady-state incremental MERGE 1.01
  to 1.42s. Converted because the work package specified it, not because the table's own economics
  called for it; the entry states plainly that a scheduled full-refresh would have been the simpler,
  equally fast choice at this row count.
- `fct_billing_transactions` (1,500,100 rows): full-refresh CREATE TABLE 3.29 to 5.36s; incremental
  MERGE with no new data 0.64 to 0.97s. Same conclusion: full refresh was already fine, and the
  incremental conversion added a `run_query` pre-query workaround, a join-based watermark recovery,
  and backfill var plumbing that a full-refresh model needs none of.
- `fct_daily_subscription_snapshot` (27,011,346 rows, periodic snapshot): full-refresh 45.12s;
  incremental MERGE touching only the 3-day reprocess window, 6.19 to 10.37s. Judged worth it, but
  for a different reason than raw size: a periodic snapshot's row count grows by a fixed amount
  every day for the life of the pipeline, so a full-refresh's cost is O(total accumulated history)
  and keeps climbing on every future run, while this incremental design's cost is
  O(reprocess_window_days x roster size) and stays flat regardless of history depth.
- `fct_playback_events` (~119.6M rows): the project's largest fact, explicitly named by its own work
  package as the one table where incremental processing should earn its complexity, unlike the
  smaller facts. Full-refresh at this scale is minutes, not seconds, and this project's own
  single-node Trino already required specific memory tuning (narrowed column projection, session
  properties) just to complete a full-width scan of this table without exceeding its 1.5GB
  per-query cap.

`fct_signup_funnel` and every dimension (`dim_plan`, `dim_device`, `dim_date`, `dim_title`,
`dim_subscriber`, `dim_payment_method`) stayed full-refresh.

## Decision

Convert `fct_playback_events`, `fct_billing_transactions`, `fct_watchlist_adds`, and
`fct_daily_subscription_snapshot` to incremental Iceberg MERGE. Leave `fct_signup_funnel` and every
dimension as full-refresh.

The rule this project takes away, stated directly in the `fct_billing_transactions` conversion
entry: incremental buys real value once a table's full-rebuild cost is large relative to its
per-run delta, either in wall time or in resource headroom against this stack's tight per-node
memory cap. Below that crossover, incremental mostly just adds surface area, and full refresh is
the right default until growth or a real trickle-load pattern says otherwise. For a table whose
size grows without bound over the project's lifetime (a periodic snapshot, or a genuinely
open-ended event fact), that crossover gets crossed anew on every future run, not just once, which
is what justifies paying the complexity cost even when today's wall-clock numbers alone would not
force the decision.

## Alternatives Considered

- **Convert every fact, including `fct_signup_funnel`, for consistency.** Considered implicitly by
  omission but not pursued: `fct_signup_funnel` has no documented crossover justification recorded
  anywhere in this project's build history, and converting it would have added the same watermark
  and backfill machinery as the other conversions for a table with no stated need for it, on the
  same reasoning that made `fct_watchlist_adds` and `fct_billing_transactions`'s own conversions an
  honest "not really worth it, converted because specified" call.
- **Convert dimensions too.** Explicitly deferred from the start (`dim_plan`, `dim_title`,
  `dim_subscriber` were all built full-refresh "correctness first, incrementality later" per an
  early, repeated precedent) and never revisited within this project's scope. `dim_subscriber` in
  particular has real per-build mechanics (the late-arriving self-heal step, the Type 3 plan-segment
  cursor) that a full-refresh build already had to solve without a persisted prior state to diff
  against; converting it to incremental would mean re-deriving those mechanics against an
  incremental diff instead, a nontrivial redesign, not a materialization flag flip.
- **Size-only threshold** (convert every fact above some fixed row count). Rejected implicitly by
  the periodic-snapshot reasoning: `fct_daily_subscription_snapshot` was judged worth converting
  more for its unbounded-growth shape than for its size at conversion time, which a size-only rule
  would have missed or gotten right only by coincidence.

## Consequences

- Four different watermark and backfill mechanisms now exist across the four converted facts, not
  one uniform pattern: `fct_watchlist_adds` and `fct_billing_transactions` recover their watermark
  via a join back to silver, `fct_playback_events` reads it from Iceberg's own `$snapshots` metadata
  instead (a join at that row count was independently confirmed to risk the same memory cap the
  table's own uniqueness test had already crashed), and `fct_daily_subscription_snapshot` has no
  source row to watermark against at all and instead recomputes a `[range_start_date,
  range_end_date]` window every run. A future maintainer touching one of these cannot assume another
  one's pattern applies.
- `on_schema_change='fail'` is set on every incremental model, turning any future silver column
  change into a hard build failure instead of the full-refresh version's silent absorb-and-move-on.
  This is a real, stated cost of the conversion, not an oversight.
- Dimensions remain a single point of full-refresh recomputation on every build. Any future decision
  to convert a dimension needs to solve how its own per-build mechanics (self-heal, SCD cursors)
  translate to an incremental diff, which this project's fact conversions do not answer.
