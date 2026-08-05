# ADR-004: Ingestion-time watermark, not event-time, for incremental processing

## Status

Accepted, 2026-08-04.

## Context

Every incremental fact conversion in this project needed a way to identify which silver rows are
new since the last run. The obvious candidate is the row's own business event timestamp (for
example `session_started_at` on playback, `transaction_posted_at` on billing, `added_at` on
watchlist): filter for rows newer than the last run's maximum event time.

This project's synthetic data generator deliberately injects out-of-order arrival as one of its
required pathologies: a fraction of rows in every playback batch (`out_of_order_fraction`, 3%) are
peeled off and spliced into a file written 3 to 15 batches later than the batch their own
`session_started_at` belongs to. The generator's own manifest
(`_pathology_manifest/out_of_order_summary.json`) and `sanity_check.py` treat this as a required,
verified property of the dataset, not an incidental side effect.

## Decision

Every incremental fact's `is_incremental()` filter is built on bronze ingestion time (`_ingested_at`,
passed through silver unchanged), never on the row's own business event time.

Silver rebuilds full-refresh every run but preserves each row's original bronze `_ingested_at`
untouched, so `_ingested_at > watermark` correctly identifies rows genuinely new to bronze on this
run, regardless of how old their business event time happens to be. A watermark on event time would
permanently and silently drop any row whose event time falls inside a range an earlier run already
scanned, which is exactly what the out-of-order pathology produces on purpose: once a watermark on
`session_started_at` has advanced past a straggler's true event time, that straggler would never be
picked up by any later incremental run, no error, no trace.

This was verified directly against `fct_playback_events`, not just reasoned about: the built table's
own `max(session_started_at)` already sits at or past the value a straggler's event time would need
to clear, so an event-time watermark would have excluded it silently, while `_ingested_at` carries no
event-time information at all and so cannot make that mistake.

## Alternatives Considered

- **Watermark on business event time** (`session_started_at`, `transaction_posted_at`, `added_at`).
  The more conventional choice for an incremental fact and the one that reads more naturally against
  the fact's own grain. Rejected because it silently drops exactly the class of late-arriving,
  out-of-order row this project's generator deliberately produces to test for, which would make the
  incremental conversion demonstrably wrong against this project's own data rather than merely
  theoretically fragile.
- **A dual watermark** (event time for the common case, a separate reconciliation pass on
  ingestion time to catch stragglers). Not pursued: it would recover correctness at the cost of
  running two filters and two code paths per incremental model, for no benefit over simply using the
  ingestion-time watermark alone, which is correct by construction and needs only one filter.

## Consequences

- `_ingested_at` is bronze and silver bookkeeping, deliberately excluded from every gold fact's
  column contract in `modeling.md`. None of the four converted facts persist it, which means the
  watermark itself has to be reconstructed at build time rather than read directly off the target
  table (see ADR-010 for the mechanism this required and the Trino-specific limitation that forced
  it).
- A watermark that never looks at event time also never distinguishes "genuinely new data" from "old
  data that happened to load late," which is the correct behavior here but is a real semantic
  difference from what a reader expecting a conventional event-time incremental filter would assume;
  documented on each converted model rather than left implicit.
- This project's current bronze history is a single ingestion batch per table for most facts, so the
  out-of-order pathology is the only real evidence available that the design is correct; a second,
  genuinely trickling ingestion run has not been exercised against these watermarks yet.
