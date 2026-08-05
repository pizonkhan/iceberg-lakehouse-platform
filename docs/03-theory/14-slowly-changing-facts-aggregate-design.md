# Slowly changing facts and aggregate design

This document covers two problems that only surface once a dimensional model is actually queried
over time and at scale, not at build time: what happens when a fact recorded for a past event
turns out to be wrong, and what happens when a single measure column gets summed across a
dimension it was never meant to be summed across. Both are consequences of the same underlying
fact: a fact table is not just "the numbers," it is a set of numbers tied to a specific grain, and
both problems appear the moment someone (a person or a query) forgets what that grain was.

Column-level source of truth: `.notes/modeling.md`, the `fct_billing_transactions`,
`fct_daily_subscription_snapshot`, `fct_playback_events`, and `bridge_title_genre` sections. Real
model code: `transform/lakehouse/models/marts/facts/fct_billing_transactions.sql`,
`fct_daily_subscription_snapshot.sql`, `fct_playback_events.sql`, `bridge_title_genre.sql`. The
incremental MERGE mechanics referenced throughout part one are covered in full in
`docs/03-theory/07-merge-semantics-idempotency.md`; this document does not repeat that material,
it cites the specific results relevant to restatement.

## Part one: slowly changing facts and the restatement problem

### The problem, stated precisely

A slowly changing dimension is about an attribute of an entity changing: a subscriber upgrades
from `standard` to `premium`, and `dim_subscriber` needs to decide whether to overwrite that
attribute in place or version it. A **slowly changing fact** is a different problem entirely: it
is not an entity's attribute changing, it is a measurement of something that already happened
turning out to have been wrong, or incomplete, when it was first recorded. A subscriber disputes a
charge and gets a partial refund three days later. A billing system posts a correction because the
original invoice used a stale price. A snapshot process computed a subscriber's plan at end of day
before that day's last billing event had actually landed, and the true state of that day is only
knowable in retrospect. In every case, the underlying event already happened at a fixed point in
time, but the organization's recorded belief about that event's value changes after the fact.

The two entity-change strategies dimensions use (Type 1 overwrite, Type 2 version) do not map
cleanly onto this problem, because a fact does not have "attributes" to overwrite the way a
dimension does: a fact row is a measurement, and correcting a measurement is either an edit to
history or an admission that history was never one row to begin with. That choice has to be made
deliberately, because it determines whether the system can ever answer "what did we believe about
this on the day we first recorded it" once a correction arrives.

### The mechanism: two competing strategies

**Overwrite the original fact row in place.** The correction becomes an `UPDATE` against the
original row's `amount_usd` (or whatever measure changed), and the store contains, at all times,
only the corrected values. This has an appealing property: a query summing revenue for last
quarter always returns the current best-known answer with no extra logic. It has a fatal one for
any system that needs an audit trail: the moment the update commits, the fact that a correction
ever happened is gone. There is no way to answer "what did the revenue report say on the day it
was published," which is exactly the question a finance team asks when a customer disputes a
number that was already reported externally, or when a discrepancy between two historical reports
needs to be explained. Overwrite-in-place also conflates two different real-world events (the
original charge, and the later correction) into one row, which is a grain violation in the same
sense a fan trap is: one row is now standing in for two distinct business facts.

**Append a new adjusting fact row.** The correction is never applied to the original row at all;
it lands as its own new row, with its own posting time and its own signed amount, and the "true"
value of anything derived from this fact is whatever a query over the full row set computes. This
keeps every fact table immutable and append-only (write once, never update), which is the same
posture bronze already takes for the exact same audit reason: this project's bronze layer is
defined as raw and append-only, never modified, fully replayable. The cost is that no single row answers "what is the current
value"; every consumer has to aggregate. That cost turns out to be small for a well-chosen grain
and a well-chosen sign convention, which is exactly what the next section shows.

### What this project actually does: `fct_billing_transactions` is append-only

`fct_billing_transactions`'s grain, per `.notes/modeling.md`, is "one row per discrete billing
ledger event (charge, refund, credit, or proration) posted to one subscriber's account." A refund
is not a correction to the charge row that preceded it; it is its own event, with its own
`billing_transaction_id`, its own `transaction_type`, and its own signed `amount_usd`. The model
code never updates an existing row's `amount_usd` in response to a refund or credit; the Iceberg
`MERGE` this fact runs on (`incremental_strategy='merge'`, `unique_key='billing_transaction_id'`)
only ever inserts a genuinely new `billing_transaction_id` or re-matches an already-loaded one back
onto itself unchanged, because `silver_billing_ledger` is deduplicated on that same id upstream. A
refund event carries a different id than the charge it refunds, so it is structurally a `NOT
MATCHED` insert, never a `MATCHED` update of the charge.

The sign convention is what makes this append-only design actually usable without special-case
logic at query time. From `.notes/modeling.md`:

> Sign convention means sum(amount_usd) is net revenue with no CASE logic; a data test asserts
> refunds and credits are negative or zero, charges positive.

And the model's own header comment confirms this was verified against real data, not assumed:

```sql
-- Sign convention (modeling.md: "charges positive, refunds and credits
-- negative, prorations either sign") is a straight passthrough of
-- amount_usd and tax_amount_usd, not derived or corrected: verified
-- against the real data before writing this model that
-- silver_billing_ledger already encodes the convention exactly as
-- specified, with zero exceptions across all 1,500,100 rows (charge
-- always > 0, refund and credit always < 0, proration both signs).
```

**Worked example.** Suppose subscriber `sub_04821` is on `plan_02` (Premium monthly, real price
$17.93 from `dim_plan`) and their billing history looks like this:

| billing_transaction_id | transaction_type | transaction_posted_at | amount_usd |
|-------------------------|-------------------|--------------------------|-----------:|
| btx_1001                | charge            | 2026-06-01 00:00:00      |     +17.93 |
| btx_1002                | credit            | 2026-06-03 00:00:00      |      -5.00 |
| btx_1003                | proration         | 2026-06-15 00:00:00      |      +4.31 |
| btx_1004                | refund            | 2026-06-20 00:00:00      |     -17.93 |

`btx_1002` is a goodwill credit issued after a support call. `btx_1003` is a mid-cycle upgrade
proration. `btx_1004` is a full refund of the original charge after the subscriber cancels within
the refund window. None of these three rows ever touches `btx_1001`; each is its own row with its
own id. `sum(amount_usd)` over this subscriber's full history is a plain addition, no `CASE`
needed:

```
17.93 + (-5.00) + 4.31 + (-17.93) = -0.69
```

That is net revenue for this subscriber over the period: the platform actually owes them $0.69
net, entirely recoverable from a single unconditional `SUM`. Restatement falls out of this for
free. "What did we believe about this subscriber's revenue as of June 10" is:

```sql
select sum(amount_usd)
from fct_billing_transactions
where subscriber_id = 'sub_04821' and transaction_posted_at <= timestamp '2026-06-10 23:59:59.999999'
```

which returns `17.93 + (-5.00) = 12.93`, exactly what a report run on June 10 would have shown,
because `btx_1003` and `btx_1004` had not been posted yet. "What do we know now" is the identical
query with no date filter, returning `-0.69`. Both answers live in the same table, computed by the
same immutable rows; the only thing that changed between them is the query's own date filter, not
the storage. This is the precise sense in which the design "naturally supports restatement without
any special handling": there is no restated table, no correction log to reconcile, no second
storage strategy layered on top of the first. A restated answer is just a different `WHERE` clause
over data that was never mutated.

### The general problem class: periodic snapshot facts

An append-only transaction fact restates for free because its grain already is the event.
A **periodic snapshot fact** does not have that luxury by construction: `fct_daily_subscription_
snapshot`'s grain is "one row per subscriber per calendar day," a manufactured row that did not
exist as a discrete real-world event, it is a summary of whatever was true about that subscriber
at one instant, resolved point-in-time against `dim_subscriber` and `silver_billing_ledger`. When
one of those upstream sources is itself corrected after the fact (a subscriber's status is fixed
retroactively, a billing correction changes which plan a subscriber should be attributed to on a
given day), the snapshot row for that day is now wrong, and unlike the billing fact, there is no
way to "add a new row" that fixes it: the day already has exactly one row, by the unique key
`(snapshot_date_key, subscriber_sk)`, and a second row for the same day would violate the grain.
The only options for a periodic snapshot are to revisit and overwrite the specific historical row,
or to never revisit it and let the correction stand uncaptured in this fact (even though the
sources it was built from now disagree with it).

How the actual MERGE machinery decides which historical rows to regenerate on each run, the
watermark design, and the idempotency proof that repeated regeneration converges rather than
drifts are all covered in `docs/03-theory/07-merge-semantics-idempotency.md`; this section states
only the restatement-relevant conclusion and cites the real evidence for it.

### Does this project's snapshot fact actually get restated? Yes, within a bounded window, stated plainly

`fct_daily_subscription_snapshot` does revisit already-written history, on every single incremental
run, by design. The model's `bounds` CTE sets `range_start_date` to `reprocess_window_days`
(default 3) days before the latest already-written `snapshot_date_key`, not to "the day after the
latest snapshot." The model's own header comment states the tradeoff directly:

```sql
-- Retroactive correction of past snapshot days: DELIBERATELY handled with
-- a bounded rolling reprocessing window, not left as pure immutable
-- history. `range_start_date` is not "the day after the latest existing
-- snapshot_date_key"; it is `reprocess_window_days` (default 3, var
-- `reprocess_window_days`) days before that, so every incremental run
-- re-generates and MERGEs the most recent `reprocess_window_days` days of
-- already-written history in addition to any genuinely new days. ...
-- a correction to a day older than the window is not revisited and the
-- previously-written row stands.
```

This was proved against a constructed scenario, not just asserted (`.notes/decisions.md`, 2026-08-04
entry on this fact): a fake subscriber's 10-day history had two retroactive corrections applied to
a copy of `dim_subscriber`, one landing inside the 3-day window and one outside it. Re-running the
bounded MERGE logic picked up the in-window correction (the affected days flipped to the corrected
status) and left the out-of-window correction stale on the already-written rows, exactly as
designed. The steady-state behavior on the real 27,011,346-row table confirms the same shape in
production: three consecutive incremental runs with no new day eligible each re-touched "exactly
149,384 rows each time, spanning snapshot_date_key 20260801 through 20260803, i.e. precisely the
3-day reprocess window and nothing else," while the other roughly 26.86 million rows were never
rewritten.

The honest limit, stated in the model's own comment and worth repeating here rather than glossing
over: **a correction to a source that lands more than `reprocess_window_days` days after the
original snapshot row was written is never picked up by the normal incremental path.** The stale
row stands until someone explicitly runs the model's `backfill_start_date` / `backfill_end_date`
mechanism against that specific range. This project considered, and explicitly rejected, the
alternative of pure immutable history (never reprocessing snapshot rows at all, which is a
legitimate and common choice for a periodic snapshot fact), on the grounds that the reprocess
window costs almost nothing extra on a MERGE that already has to run every day (the reprocessed
rows are matched-and-updated by the same statement that would otherwise do nothing) while buying
protection against the most common shape of correction, one noticed within a few days of the
original write.

### When it fails

**A correction older than the reprocess window is silently not applied**, by design, not by bug.
Widening `reprocess_window_days` only pushes this boundary out; it does not remove it, and widening
it costs `O(reprocess_window_days x subscriber roster size)` extra row generation and MERGE traffic
on every single run, forever, not just once.

**An overwrite-in-place fact loses restatement entirely**, the failure mode this project's
append-only billing design avoids: if `fct_billing_transactions` instead updated `btx_1001`'s
`amount_usd` in place when the refund landed, the June 10 query above would return the corrected,
post-refund number even though the refund had not happened yet as of June 10, silently rewriting
history rather than reporting what was actually known on that date.

**A grain violation on a periodic snapshot masks a restatement problem as a duplicate-row bug.**
If a snapshot's unique key were looser than `(snapshot_date_key, subscriber_sk)`, a reprocessing
run would not update the existing day's row, it would insert a second row for the same day,
turning what should be an invisible correction into a visible, incorrect double-count the next time
someone sums `mrr_amount_usd` for that day.

### How to verify this is actually working from this repo

Reproduce the append-only claim directly: `select count(*) from fct_billing_transactions where
billing_transaction_id = 'btx_1001'` (or any real charge id) should return exactly 1, permanently,
regardless of how many refunds or credits are later posted against that same subscriber; those
land as new rows with new ids, confirmed by `select subscriber_id, transaction_type, amount_usd
from fct_billing_transactions where subscriber_id = '<id>' order by transaction_posted_at`, which
should show the full charge/refund/credit/proration sequence as distinct rows.

Reproduce the bounded-restatement claim: run `dbt build --select fct_daily_subscription_snapshot
--target trino` twice in a row with no upstream change and compare `loaded_at` before and after for
a handful of `snapshot_date_key` values. Rows inside the trailing `reprocess_window_days` days of
the table's current maximum `snapshot_date_key` will show a fresh `loaded_at` after the second run;
rows outside that window will not. This is the same check `.notes/decisions.md` used to confirm
"exactly 149,384 rows" were touched on a no-op run, reproducible on any live build of this fact.

## Part two: aggregate design

### Aggregate navigation, and whether this project implements it

Aggregate navigation is the idea that a query engine or a BI semantic layer can transparently
redirect a query to a smaller, pre-computed rollup table instead of scanning the full-grain fact,
whenever the rollup covers everything the query needs (the right grain, the right measures, no
filter on a column the rollup does not retain). Done well, it is invisible to the analyst: the same
SQL that would scan `fct_playback_events`'s ~120 million rows for "total watch time by device type
last month" instead resolves against a rollup with one row per device type per month, without the
query author writing anything differently.

This project does not implement aggregate navigation anywhere. A direct check of every model under
`transform/lakehouse/models/marts/` confirms it: the `marts/` tree contains exactly two
subdirectories, `dimensions/` and `facts/`, and every model in `facts/` is declared at its full,
original grain (`fct_playback_events` at one row per session, `fct_billing_transactions` at one row
per ledger event, `fct_daily_subscription_snapshot` at one row per subscriber per day, and so on).
There is no `marts/aggregates/` directory, no model whose name or description indicates a rollup or
summary grain, and no materialized-view or query-rewrite configuration anywhere in the dbt project
or the Trino setup that would redirect a query transparently. Every query in this project's BI
layer, ad hoc analysis, or test suite that wants an aggregate result computes it by scanning a
full-grain fact and grouping, every time. That is a real, sizeable cost at this project's scale
(`docs/03-theory/07-merge-semantics-idempotency.md` records a 242.68 second full scan of
`fct_playback_events`), and it is worth stating plainly rather than implying an aggregate layer
exists somewhere it does not: this is an area where a production system at this row count would
likely add one, and this project has not.

### Additivity: the property that decides whether a rollup is even safe to build

Aggregate navigation, if it existed here, would only be safe for a measure that can be correctly
summed by simply adding it across the rows being collapsed. Not every measure has that property,
and this project's own facts carry examples of all three standard additivity classes side by side.

**Fully additive.** `watch_duration_seconds` on `fct_playback_events` (`.notes/modeling.md`:
"measure, additive") is a fully additive measure: it sums correctly across every dimension
this fact has simultaneously, subscriber, title, device, and time. Summing it for one subscriber
across every session gives that subscriber's total watch time; summing it for one title across
every subscriber gives that title's total watch time; summing it across every day of a month gives
that month's total watch time; and any combination of those groupings still sums correctly, because
each session's watched seconds is a genuinely independent quantity that never double-represents
another session's seconds. This is the only class of measure a naive rollup (`GROUP BY` plus `SUM`)
is unconditionally safe to build for, at any combination of grouping columns.

**Semi-additive.** `mrr_amount_usd` on `fct_daily_subscription_snapshot` is additive across one
dimension (subscribers, at a fixed point in time) and not additive across another (days). The
column's own description in `_fct_daily_subscription_snapshot.yml` states this precisely, warning
against exactly the mistake an analyst unfamiliar with the measure is likely to make:

> Semi-additive measure: monthly recurring revenue contribution of this subscriber on this day, in
> US dollars. Additive across subscribers on the same day (sum across subscriber_sk for one
> snapshot_date_key is a valid total MRR for that day), but never additive across days (summing one
> subscriber's mrr_amount_usd over a date range double, triple, or N-counts the same recurring
> revenue for every day it recurred; the correct way to roll this measure up over time is an
> average or a point-in-time value on one chosen day, never a sum).

The reason is mechanical, not a matter of convention: `mrr_amount_usd` is not "revenue earned that
day," it is "the recurring monthly rate this subscriber is on, restated as of that day." The model
computes it as the resolved plan's `current_price_usd` (monthly plans passed through as-is, annual
plans divided by 12 and rounded) with `0.00` during trial or paused status. A subscriber on
`plan_02` (Premium monthly, real price $17.93) carries `mrr_amount_usd = 17.93` on every day they
hold that plan, not because $17.93 was newly earned each day, but because $17.93 is a *rate*
restated on every row. Summing across 30 such days does the arithmetic `30 x 17.93 = $537.90`, a
number with no business meaning: this subscriber did not generate $537.90 in a month on a $17.93
plan. The correct 30-day rollup is either the point-in-time value on one representative day
(`$17.93`, still their MRR) or, for a genuine "how did MRR trend" question, an average or an
explicit day-by-day series, never a `SUM`. This is exactly why a naive aggregate-navigation rule
("build a monthly rollup by summing the daily rows") would be actively wrong for this specific
measure even though it is completely correct for `watch_duration_seconds`: additivity is a
property of the measure and the dimension being summed across together, not a property of the
column alone.

**Non-additive.** `completion_pct` and `avg_bitrate_kbps`, both on `fct_playback_events`, cannot be
correctly summed across any dimension and then divided; they have to be recomputed at whatever
grain is actually wanted. `completion_pct` is a ratio (`watch_duration_seconds` over the resolved
title's runtime in seconds, capped at 1.0000); `avg_bitrate_kbps` is a session-level average. Both
are described in `_fct_playback_events.yml` explicitly as "non-additive."

The concrete arithmetic error naive summing-then-dividing produces, worked through directly:
suppose one subscriber has two playback sessions this quarter, one for a title with a 3,600-second
(60-minute) runtime watched in full (`watch_duration_seconds = 3600`, `completion_pct = 1.0000`),
and one for a title with a 7,200-second (120-minute) runtime watched half through
(`watch_duration_seconds = 3600`, `completion_pct = 0.5000`). A naive rollup that averages the two
`completion_pct` values directly gets:

```
(1.0000 + 0.5000) / 2 = 0.7500
```

The correct recomputation sums the numerator and denominator separately, then divides once, at the
grain actually wanted (this subscriber, this quarter):

```
(3600 + 3600) / (3600 + 7200) = 7200 / 10800 = 0.6667
```

`0.7500` versus `0.6667` is not rounding noise, it is an 8.3-percentage-point error, and it grows
worse the more the underlying runtimes differ. The naive average silently gives every session equal
weight regardless of how much runtime it represents, which is wrong the moment sessions span titles
of different lengths, exactly the situation this fact's own point-in-time title resolution
guarantees will happen at scale.

The same failure shape applies to `avg_bitrate_kbps`, with session duration standing in for the
missing weight. Suppose a subscriber has one 2-hour (7,200-second) session that streamed cleanly at
6,000 kbps and one 5-minute (300-second) session that degraded to 1,500 kbps due to a poor
connection. A naive unweighted average of the two session-level averages gets:

```
(6000 + 1500) / 2 = 3750 kbps
```

which reads as "moderate, degraded quality overall." A duration-weighted recomputation, giving each
session's average proportional influence to how long it actually ran, gets:

```
(6000 x 7200 + 1500 x 300) / (7200 + 300) = (43,200,000 + 450,000) / 7500 = 5,820 kbps
```

a materially better figure (5,820 versus 3,750 kbps, a 55% understatement from the naive version),
because the naive average let a 5-minute struggling session outweigh a 2-hour clean one. Even the
duration-weighted figure is only an approximation of what a true per-byte average would give, since
`avg_bitrate_kbps` itself is already a session-level average with no finer-grained bitrate samples
stored on this fact; the honest statement is that neither `completion_pct` nor `avg_bitrate_kbps`
can ever be correctly rolled up by touching only the already-aggregated column, and any rollup has
to either recompute from the two components separately (as `completion_pct` allows, since its
numerator and denominator are both present on the fact) or be treated as approximate (as
`avg_bitrate_kbps` requires, since no finer-grained component exists to recompute from).

### The bridge table: `allocation_weight` exists to preserve additivity across a many-to-many join

`bridge_title_genre` resolves a many-to-many relationship: a title can carry more than one genre
(real data, verified by direct query against the source: `12,385` (title, genre) rows across
exactly `5,000` titles, 1 to 4 genres per title). Joining a fact to this bridge on `title_id`, with
no weighting, fans a single title's fact rows out once per genre it belongs to. Any measure summed
"by genre" through that plain join is now counted once per genre row the title carries, not once
per real occurrence, which is the same fan-out failure additivity assumes cannot happen: a measure
is only safely summable when each unit of it is represented exactly once in the rows being summed,
and a plain bridge join breaks that guarantee for every multi-genre title.

`allocation_weight` restores it. From `.notes/modeling.md`: "bridge_title_genre attaches genres to
dim_title... allocation_weight summing to exactly 1.0 per title." A real title in this project's
data, `tt_00033`, carries three genre rows:

| title_id  | genre_name  | allocation_weight |
|-----------|-------------|-------------------:|
| tt_00033  | war         |             0.6992 |
| tt_00033  | documentary |             0.2467 |
| tt_00033  | drama       |             0.0541 |

(`0.6992 + 0.2467 + 0.0541 = 1.0000` exactly, matching the bridge's own data test on every one of
the real 5,000 titles.)

**Worked example.** Suppose all playback sessions for `tt_00033` sum to 500,000 total
`watch_duration_seconds` over some period. A weighted query, multiplying that measure by
`allocation_weight` before summing by genre, attributes:

```
war:         500,000 x 0.6992 = 349,600 seconds
documentary: 500,000 x 0.2467 = 123,350 seconds
drama:       500,000 x 0.0541 =  27,050 seconds
                                --------
                        total:  500,000 seconds
```

The three genre buckets sum back to exactly 500,000, the true total: additivity is preserved
across the many-to-many join. An **unweighted** query, one that joins the fact to the bridge on
`title_id` and sums `watch_duration_seconds` grouped by `genre_name` with no multiplication, instead
attributes the full, unweighted 500,000 seconds to *each* of the three genre rows, because the join
fans one fact row out into three:

```
war:         500,000 seconds (full amount, not scaled)
documentary: 500,000 seconds (full amount, not scaled)
drama:       500,000 seconds (full amount, not scaled)
                                --------
                        total: 1,500,000 seconds
```

a 3x overcount of this one title's real contribution, growing to however many genres any given
title carries (up to 4 in this project's real data). This is exactly the risk `.notes/modeling.md`
names directly: "Weighted fact queries multiply the measure by allocation_weight to avoid double
counting; unweighted 'any exposure' queries must deduplicate and analysts should be warned of that
in the model description." An unweighted query is not always wrong to run, "how many distinct
titles tagged documentary did this subscriber watch" is a legitimate unweighted question, but
summing a measure through this bridge without either the weight or an explicit dedup step is wrong
every time, silently, for any title with more than one genre.

### When it fails

**A rollup that sums a semi-additive measure across its non-additive dimension is wrong regardless
of how the SQL is written.** No amount of correct `GROUP BY` syntax fixes `sum(mrr_amount_usd)`
across a date range; the fix is choosing a different aggregation (an average, or a single day's
value), not a different query shape over the same aggregation.

**A rollup built once and reused later silently drifts from a corrected source**, the same failure
mode part one covers for the base fact, now compounded: if this project ever did add an aggregate
table, it would need its own restatement policy (rebuild on every source correction, or accept
staleness within a bounded window, exactly the choice `fct_daily_subscription_snapshot` already
had to make for its own base rows), and a naive one-time materialization with no reprocessing logic
at all would silently disagree with the full-grain fact the moment a correction landed underneath
it.

**A many-to-many join through a bridge with no weighting looks correct until a multi-member entity
is queried.** A query built and tested against a title with exactly one genre will look identical
whether or not `allocation_weight` is applied; the bug only appears once a title with more than one
genre enters the result set, which makes it easy to ship and validate against the wrong sample.

### How to verify this is actually working from this repo

Confirm no aggregate navigation exists: `find transform/lakehouse/models/marts -maxdepth 1 -type d`
lists only `dimensions` and `facts`; grep the marts tree for `materialized='materialized_view'` or
any rollup-shaped model name and confirm zero matches.

Confirm the semi-additive warning is live: `select column_name, description from
information_schema.columns` is not how dbt docs are stored, but `dbt docs generate && dbt docs
serve` against this project renders the exact column description quoted above for
`mrr_amount_usd`, sourced from `_fct_daily_subscription_snapshot.yml`.

Reproduce the bridge additivity check directly: `select title_id, sum(allocation_weight) from
bridge_title_genre group by title_id having sum(allocation_weight) != 1.0000` should return zero
rows; this is the same test already declared on the model
(`dbt_expectations.expression_is_true`, `sum(allocation_weight) = 1.0000` grouped by `title_id`).
Reproduce the double-counting risk directly by comparing `select sum(watch_duration_seconds) from
fct_playback_events f join dim_title dt on dt.title_sk = f.title_sk where dt.title_id = 'tt_00033'`
(the true total) against the same query joined further through `bridge_title_genre` with no
weighting and no genre filter (which will return exactly 3x the true total for this specific
title, since it carries three genre rows), then against the weighted version multiplying by
`allocation_weight` before summing (which returns the true total back).
