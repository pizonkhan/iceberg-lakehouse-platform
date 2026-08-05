# SCD types

A dimension's attributes change over time (a subscriber upgrades their plan, a title's maturity
rating gets corrected, a plan's price changes). A slowly changing dimension (SCD) type is a rule
for what a dimension does when the source shows a new value for an attribute it already has a row
for: overwrite it, version it, keep one prior value alongside it, or some combination. This
document covers all eight standard types, 0 through 7. Four are implemented in this project's
real models and are covered with the actual SQL and real numbers: Type 1 (`dim_device`), Type 2
(`dim_title`), Type 3 (`dim_plan`), and Type 6 hybrid (`dim_subscriber`). Four are not implemented
here (Types 0, 4, 5, 7); those sections explain the mechanism from first principles and state
plainly that this project does not use them, with the actual reason, rather than inventing an
in-repo example that does not exist.

Column-level source of truth: `.notes/modeling.md`. Real model code: `dim_device.sql`,
`dim_title.sql`, `dim_plan.sql`, `dim_subscriber.sql` under
`transform/lakehouse/models/marts/dimensions/`.

## Type 0: retain original

**Semantics.** The attribute is written once, at first load, and is never updated again for the
life of the row, regardless of what the source shows on later loads. It is the "write-once"
extreme: not versioned (no new row appears), not overwritten (no update touches it either). It
exists for attributes whose entire analytical value is the original value, things like "the
credit score at signing" or "the first plan a subscriber ever chose," where later corrections in
the source are considered noise relative to the question being asked, not truth to reconcile.

**Why this project does not use it.** No dimension in this project's approved plan is assigned
Type 0 as its grain. Every dimension that needs to preserve a value against later overwrites
either versions the whole row (Type 2 or Type 6) or is small and static enough (Type 1, Type 3)
that "retain original forever, ignore all later loads" was never a requirement anyone specified.
It is worth being precise about a real but narrower case in this repo that looks similar without
being a Type 0 dimension: `dim_subscriber.signup_date` is documented as "attribute, static, NULL
only on inferred rows" and set once from the subscriber's `change_type = 'signup'` event, never
revised afterward. That is a single Type-0-flavored *column* living inside a Type 6 dimension, not
a dimension whose whole grain is Type 0; the project has never assigned a dimension that type, and
`signup_date` is treated as an ordinary static attribute in the model contract, not as an
instance of a formally implemented SCD type.

## Type 1: overwrite (dim_device)

**The problem it solves.** Some attributes have no history worth keeping, either because the
source itself has no history (corrections replace bad data, they are not a new fact about time)
or because no consumer needs to know what the value used to be. Versioning these attributes would
only inflate row count for no analytical benefit.

**The mechanism.** Each build reads the current state of every entity from source and replaces
the dimension's row for that entity outright. There is no `effective_from`, no `effective_to`, no
`scd_version`. One entity, one row, always.

**Real SQL**, `dim_device.sql` in full (Type 1, current state only, corrections overwrite):

```sql
with silver_devices as (
    select
        device_id,
        device_type,
        manufacturer,
        model_name,
        os_name
    from {{ ref('silver_devices') }}
),

unknown_member as (
    select
        '-1' as device_id,
        'Unknown' as device_type,
        'Unknown' as manufacturer,
        'Unknown' as model_name,
        'Unknown' as os_name
),

unioned as (
    select * from silver_devices
    union all
    select * from unknown_member
)

select
    {{ dbt_utils.generate_surrogate_key(['device_id']) }} as device_sk,
    device_id,
    device_type,
    manufacturer,
    model_name,
    os_name,
    device_type in ('mobile', 'tablet') as is_mobile,
    cast({{ dbt.current_timestamp() }} as timestamp(6)) as loaded_at
from unioned
```

The theory-to-code connection is direct: there is no CTE anywhere in this file that compares an
incoming row to a previously materialized row, because Type 1 has nothing to compare against, it
simply selects the current source state every time. A full-refresh build of a Type 1 dimension is
indistinguishable from "there was never a prior version"; that is the entire mechanism.

**Storage growth.** Row count equals entity count, always, independent of how often
`device_type`, `manufacturer`, `model_name`, or `os_name` change. Real data confirms this
directly: `.notes/decisions.md`, 2026-08-04, "`silver_devices` has exactly 3,000 rows / 3,000
distinct device_id values... dim_device built to 3,001 rows (3,000 real devices plus the one
unknown member)." Whether a device's `manufacturer` has been corrected zero times or a hundred
times, the row count contribution from that device is exactly one, forever.

**Query complexity.** A plain filter or plain join, no time predicate:

```sql
select device_type, count(*) as sessions
from fct_playback_events f
join dim_device d on d.device_sk = f.device_sk
group by device_type;
```

There is exactly one `device_sk` per `device_id` for all time, so any join resolves unambiguously
regardless of when the fact event happened.

**When it fails, and the counter-indications.** Type 1 is wrong whenever a downstream question
needs the attribute's value *as of the event*, not its current value. If device manufacturers were
ever corrected retroactively and an analyst asked "what was the device manufacturer breakdown of
sessions in March," a Type 1 dimension would silently answer with today's corrected manufacturer
values applied to March's sessions, not March's real values. This project accepts that risk for
`dim_device` deliberately: modeling.md assigns it Type 1 because "silver_devices is static
reference data with no versioning to preserve" (per the model's own header comment), i.e. the
source itself does not carry change history for device attributes, so there is nothing for Type 1
to lose that Type 2 could have kept.

**Verifying it.** `_dim_device` tests (`not_null`/`unique` on `device_sk`) confirm the grain holds
(one row per `device_id`). Idempotency check from `.notes/decisions.md`, 2026-08-04: "two
consecutive `dbt build --select dim_device` runs against unchanged silver input produced
byte-identical output (row count 3,001 both runs...)." Because a Type 1 dimension has no version
history to break, the only thing to verify is that the row count stays pinned to entity count
build over build, which this idempotency check demonstrates directly.

## Type 2: add new row (dim_title)

**The problem it solves.** Some attributes must be reconstructible as of any past instant: a
title's maturity rating or runtime as it was when a specific playback session happened, not as it
is today. Overwriting (Type 1) destroys that; Type 2 preserves it by inserting a new dimension row
every time a tracked attribute changes, and giving each row a validity interval.

**The mechanism.** Grain (from `dim_title.sql`'s own header comment): "one row per title per
metadata version, where a version begins whenever any of the seven tracked attributes changes."
Interval semantics are half-open `[effective_from, effective_to)`. The current row's
`effective_to` is the literal high date `9999-12-31 23:59:59.999999`, never `NULL`, so a
point-in-time join never needs a `COALESCE`. `is_current` is true exactly when `effective_to`
equals the high date. `scd_version` is 1-based per natural key in `effective_from` order.

**Real SQL.** Change detection and version-boundary detection, `dim_title.sql`:

```sql
tracked as (
    select
        title_id,
        changed_at,
        title_name,
        content_type,
        release_year,
        runtime_minutes,
        maturity_rating,
        original_language,
        is_original,
        {{ dbt_utils.generate_surrogate_key([
            'title_name',
            'content_type',
            'release_year',
            'runtime_minutes',
            'maturity_rating',
            'original_language',
            'is_original'
        ]) }} as row_hash
    from deduped
    where _tie_rank = 1
),

changes as (
    select
        title_id,
        changed_at,
        ...,
        row_hash,
        lag(row_hash) over (
            partition by title_id order by changed_at
        ) as _prev_row_hash
    from tracked
),

versions as (
    select title_id, changed_at as effective_from, ..., row_hash
    from changes
    where _prev_row_hash is null or row_hash <> _prev_row_hash
),

scd as (
    select
        title_id,
        effective_from,
        coalesce(
            lead(effective_from) over (partition by title_id order by effective_from),
            timestamp '9999-12-31 23:59:59.999999'
        ) as effective_to,
        row_number() over (partition by title_id order by effective_from) as scd_version,
        ...,
        row_hash
    from versions
),

history as (
    select
        {{ dbt_utils.generate_surrogate_key(['title_id', 'effective_from']) }} as title_sk,
        title_id,
        ...,
        effective_from,
        effective_to,
        effective_to = timestamp '9999-12-31 23:59:59.999999' as is_current,
        scd_version,
        row_hash,
        cast(current_timestamp as timestamp(6)) as loaded_at
    from scd
)
```

The connection to theory: `row_hash` gates whether a source event opens a new version at all
(`where _prev_row_hash is null or row_hash <> _prev_row_hash` in the `versions` CTE); `lead(...)`
in the `scd` CTE derives each version's `effective_to` from the *next* version's `effective_from`,
which is exactly what makes the intervals contiguous and non-overlapping by construction, not by a
separate reconciliation step. `row_hash` here is md5 over the seven tracked columns in a fixed
order (title_name, content_type, release_year, runtime_minutes, maturity_rating,
original_language, is_original), the change-detection mechanism this document only touches in
passing; the full mechanics of how `row_hash` is computed and what it deliberately excludes are
covered in a separate theory doc on change detection.

**The tie-break rule for same-instant changes.** Modeling.md:

> Tie-break rule for multiple same-day changes: `effective_from` is the source change timestamp
> from silver at microsecond precision, so distinct intraday changes become distinct sub-day
> versions... When two changes for the same natural key carry the identical timestamp, order them
> by (bronze `_batch_id`, source row sequence within the batch) and keep only the last: earlier
> same-instant states are discarded before versioning, never emitted. Zero-width intervals `[t,
> t)` are therefore impossible and a data test asserts effective_from < effective_to on every row.

`dim_title.sql` implements this in its `deduped` CTE, using `change_event_id` as the "source row
sequence within the batch" proxy documented in `.notes/decisions.md` (2026-08-04: "no explicit
'row sequence within batch' column exists on silver_title_events for the same-instant tie-break
modeling.md calls for. Used change_event_id as the stand-in: verified it is assigned monotonically
at ingestion..."):

```sql
deduped as (
    select
        *,
        row_number() over (
            partition by title_id, changed_at
            order by _batch_id desc, change_event_id desc
        ) as _tie_rank
    from source
)
```

followed by `where _tie_rank = 1` in `tracked`, keeping only the last of any exact-timestamp tie
before `row_hash` or version detection ever sees the row.

**Storage growth as a function of change frequency.** Let `n` be the number of distinct entities
and `c_i` the number of tracked-attribute changes entity `i` has undergone since the dimension's
inception. Each entity starts with one version and gains exactly one more row per subsequent
tracked change, so total (non-unknown) rows `R = n + sum(c_i)`, and the average rows-per-entity
multiplier is `R / n = 1 + mean(c_i)`. This grows without bound as change frequency rises; there is
no ceiling built into Type 2 the way there is for Type 3 below.

The real numbers for `dim_title`, from `.notes/decisions.md` (2026-08-04): `silver_title_events`
has 15,098 raw change events across 5,000 distinct `title_id` values, but `dim_title` builds to
9,256 real history rows (plus the one unknown member), because "of the 10,098 non-catalog_add
events, only 4,256 actually change at least one of the seven tracked columns versus their
immediate predecessor... the other 5,842 are exact repeats." That gives `n = 5,000`, `R = 9,256`
(matching `n + sum(c_i) = 5,000 + 4,256`), and a multiplier of `9,256 / 5,000 = 1.8512`, i.e. an
average of `0.8512` genuine tracked changes per title since catalog entry. The row-hash change
detector is exactly what prevented the raw 15,098-event count from becoming 15,098 dimension rows:
5,842 events changed nothing in the tracked set and were correctly collapsed away, and this row
count divergence from modeling.md's illustrative "~15k" estimate is logged as an expected
row-count surprise, not a bug (decisions.md: "the grain itself is unambiguous and correctly
implemented, only the illustrative count in modeling.md turned out optimistic").

**Query complexity: the point-in-time interval join.** A Type 2 dimension cannot be joined to a
fact on the natural key or on `is_current` if the question is about the fact's own event time; it
needs the actual literal predicate this project uses everywhere an SCD dimension is resolved
(modeling.md, "Point-in-time join rule"), shown here for `dim_title` via its identical pattern to
the documented `dim_subscriber` case:

```sql
from silver_playback_sessions as f
left join dim_title as dt
  on dt.title_id = f.title_id
  and f.session_started_at >= dt.effective_from
  and f.session_started_at <  dt.effective_to
```

Because the intervals are half-open, contiguous, and non-overlapping (enforced by the dimension's
own tests), this predicate matches at most one row per fact row; modeling.md notes "a post-load
test asserts fact row counts are unchanged by the join" as the concrete proof that the join stays
1:0-or-1.

**When it fails, and the counter-indications.** Type 2 is the wrong tool when either the source
has no meaningful change history to preserve (Type 1's case, `dim_device`), or when the row-count
growth from versioning the whole row is disproportionate to what a consumer actually needs
tracked (this is exactly the argument for Type 6 below: `dim_subscriber` tracks only `plan_tier`
and `status` at Type 2 granularity, not every column, because most of its columns don't need
point-in-time fidelity). A Type 2 dimension also cannot answer "as of event time" cheaply if a
consumer forgets the interval predicate and instead filters on `is_current`, which silently
answers a different, wrong question (today's metadata applied to a historical event) without
erroring.

**Verifying it.** Real tests from `_dim_title.yml`:

```yaml
tests:
  - dbt_utils.mutually_exclusive_ranges:
      lower_bound_column: effective_from
      upper_bound_column: effective_to
      partition_by: title_id
      gaps: not_allowed
      zero_length_range_allowed: false
  - dbt_utils.unique_combination_of_columns:
      combination_of_columns: [title_id, effective_from]
  - dbt_utils.unique_combination_of_columns:
      combination_of_columns: [title_id]
      config:
        where: "is_current"
  - dbt_expectations.expect_column_distinct_count_to_equal_other_table:
      column_name: title_id
      compare_model: ref('dim_title')
      compare_column_name: title_id
      compare_row_condition: "is_current"
```

Together these prove no gap or overlap between consecutive versions, exactly one current row per
`title_id`, and no `title_id` silently missing a current row. Idempotency confirmed in
`.notes/decisions.md`, 2026-08-04: two consecutive `dbt build --select dim_title` runs against
unchanged silver produced a byte-identical full column dump (excluding `loaded_at`) ordered by
`(title_id, effective_from)`.

## Type 3: add new column (dim_plan)

**The problem it solves.** Some attributes have exactly one prior value worth keeping directly
alongside the current one, for a simple before/after comparison, without wanting the row explosion
of full Type 2 versioning. Type 3 answers "what changed, and from what" for the single most recent
change only.

**The mechanism.** Grain: one row per entity, permanently. On a tracked attribute's change,
`current_x` takes the new value, `previous_x` takes the old `current_x` value, and an
`x_change_date` column records when. No new row, no SCD tracking columns (`effective_from`,
`effective_to`, `is_current`, `scd_version` do not exist on a Type 3 dimension: only one step of
history survives, which a validity interval would misrepresent as more than it is).

**Real SQL**, `dim_plan.sql`:

```sql
plans as (
    select
        {{ dbt_utils.generate_surrogate_key(['plan_id']) }} as plan_sk,
        plan_id,
        plan_name,
        current_tier,
        cast(null as varchar) as previous_tier,
        cast(null as date) as tier_change_date,
        current_price_usd,
        cast(null as decimal(8, 2)) as previous_price_usd,
        cast(null as date) as price_change_date,
        billing_period,
        max_concurrent_streams,
        video_quality,
        has_ads,
        supports_downloads
    from silver
)
```

This shows the theory-to-code connection with an honest wrinkle worth stating rather than
smoothing over: `previous_tier`, `tier_change_date`, `previous_price_usd`, and
`price_change_date` are hardcoded `NULL` here, not derived, because this build has nothing to diff
against yet. The model's own header comment states why:

> `silver_plans` carries only current plan state (no price/tier history reaches silver, per the
> source generator), so this build has no prior gold state to diff against. `previous_tier`,
> `previous_price_usd`, `tier_change_date`, and `price_change_date` are therefore structurally
> NULL here, not derived... They will only start populating once `dim_plan` is converted to an
> incremental model that diffs an incoming silver row against this same model's previously
> materialized row for that `plan_id`, a later work package's job.

This is the mechanism made visible by its own absence: Type 3's "shift current into previous"
behavior is fundamentally a *diff against prior state*, which requires either an incremental load
comparing against the table's own last materialization, or a source that carries the prior value
directly. A full-refresh build reading from a source with no history has structurally nothing to
diff, so the columns exist (satisfying the contract) but stay `NULL` (honestly reporting "no
change observed by this pipeline yet," per `.notes/decisions.md`, 2026-08-04) until the model
converts to incremental.

**Storage growth as a function of change frequency: none.** This is the direct contrast with Type
2. Row count is `R = n`, exactly, regardless of how many times any plan's tier or price has
changed. A plan that has changed price ten times and a plan that has never changed price both
contribute exactly one row to `dim_plan`. Real data: `.notes/decisions.md`, 2026-08-04, "silver_plans
has exactly 30 rows / 30 distinct plan_id values... dim_plan built to 31 rows (30 real plans plus
the one unknown member)." Compare this directly to `dim_title`'s `9,256 / 5,000 = 1.8512` rows per
entity: `dim_plan`'s ratio is fixed at `1.0` by construction, because Type 3 spends storage on
columns per entity, not on rows per change.

**Query complexity.** A Type 3 dimension needs no interval join at all, the same as Type 1, because
there is exactly one row per `plan_id` for all time:

```sql
select plan_id, current_tier, previous_tier, tier_change_date
from dim_plan
where previous_tier is not null;
```

But unlike Type 1, a single query can compare before and after: `current_price_usd` versus
`previous_price_usd` in the same row, with no join at all.

**When it fails, and the counter-indications.** Type 3 fails the moment a question needs more than
one step back. Modeling.md states this as a deliberate, accepted limitation, not a discovered bug:

> Point-in-time caveat, stated so nobody rediscovers it later: `dim_plan` cannot answer "price at
> event time" beyond one change back. Historical billing amounts live on
> `fct_billing_transactions` as posted, which is the authoritative record; the Type 3 columns
> exist for before/after comparison queries, not for revenue reconstruction.

This is why `fct_billing_transactions.amount_usd` is stored as posted at transaction time rather
than reconstructed from `dim_plan`'s current/previous columns: Type 3 is not meant to substitute
for an authoritative transactional record when more than one historical step matters. The
project's open items list flags the same limit explicitly: "If plan prices change more than once
inside the modeled window, before/after analysis of the earlier change is lost by design."

**Verifying it.** Idempotency from `.notes/decisions.md`, 2026-08-04: "Two consecutive `dbt build
--select dim_plan` runs against unchanged silver input produced byte-identical output on every
column except `loaded_at`." Surrogate key cross-check from the same entry: `plan_sk` for
`plan_00` equals `md5('plan_00') = 42b29552561d07fa699a8f0d388357e1`, confirmed independently
against `hashlib`. A direct query to confirm the constant-row-count claim itself:

```sql
select count(*) as total_rows, count(distinct plan_id) as distinct_plans
from dim_plan;
-- expect total_rows = distinct_plans (every plan contributes exactly one row)
```

## Type 4: history table

**The problem it solves.** When a dimension attribute changes very frequently, versioning the
whole row (Type 2) on every change can bloat the main dimension table to the point where ordinary
current-state queries against it get slow, even though most queries only ever want the current
row. Type 4 splits the dimension in two: a small, current-state-only table (functionally Type 1)
that stays fast for the common case, and a separate history table that receives a full log of
every change, consulted only when a point-in-time question is actually being asked.

**Why this project does not use it.** Type 4 solves a scale problem this project does not have.
The two dimensions that need historical fidelity, `dim_subscriber` and `dim_title`, are small
(125,616 and 9,257 rows respectively) and their Type 2/6 history lives directly in the main
dimension table without materially hurting current-state query performance at that row count. A
Type 6 hybrid, `dim_subscriber`'s actual assignment, achieves the same "fast current-state access"
goal Type 4 chases by mirroring current attribute values (`current_plan_tier`) directly onto every
row of the same table, rather than by physically splitting current state into a second table. No
dimension here needed a second, decoupled table to keep current-state queries fast.

## Type 5: mini-dimension with Type 1 outrigger

**The problem it solves.** When only a handful of a dimension's attributes change often, and those
attributes are low-cardinality, Type 2-versioning the entire wide dimension row for every one of
those changes wastes storage on the other, rarely-changing columns getting needlessly repeated
across versions. Type 5 splits the volatile, low-cardinality attributes into a small separate
"mini-dimension" (itself Type 2 or a simple combination table), and keeps the base dimension as
Type 1 with a foreign key ("outrigger") pointing at the mini-dimension's current row.

**Why this project does not use it.** The one place this pattern would apply, `dim_subscriber`'s
`plan_tier` and `status`, are exactly the two columns this project tracks at Type 2 granularity,
but the project keeps them inside the base dimension row rather than splitting them into a
separate mini-dimension table. That works here because the tracked set is already narrow (two
columns, not the whole row) and the resulting row count (125,616) is small enough that a second
joined table buys nothing: the complexity of an outrigger join is not justified when the base
table itself is already this cheap to scan. `dim_payment_method`, this project's actual junk
dimension (a combination table for low-cardinality billing flags), is structurally close to what a
Type 5 mini-dimension looks like, but it is not an outrigger off a base dimension; it stands alone
as its own dimension referenced directly from `fct_billing_transactions`, which is a different
pattern (a junk dimension collecting flags that would otherwise sit on the fact) than Type 5's
"split off the volatile part of an existing dimension" motivation.

## Type 6: hybrid, Type 1 + 2 + 3 combined (dim_subscriber)

**The problem it solves.** A single dimension often has columns that need all three simpler
behaviors at once: some attributes should just be overwritten (Type 1), some need full
point-in-time history (Type 2), and one needs a quick before/after comparison (Type 3), and a
consumer should be able to ask either "what is true right now" or "what was true at this exact
moment" against the *same* dimension without joining to a second table or re-deriving anything.
Type 6 (the name is conventionally read as "1 + 2 + 3 = 6") combines all three behaviors in one
row shape.

**The mechanism, from first principles.** `dim_subscriber`'s grain: "one row per subscriber per
tracked-attribute version, where a version begins whenever `plan_tier` or `status` changes." Only
those two columns are Type 2 tracked (part of `row_hash`, gate new versions). Modeling.md's
maintenance rule, stated exactly:

> Type 6 maintenance on a plan_tier change: close the current row, insert the new version, then
> overwrite `current_plan_tier` on every historical row of that subscriber with the new value, and
> set `previous_plan_tier` on the new row to the `plan_tier` of the row just closed.
> `previous_plan_tier` on historical rows is left as written at their creation; only the newest
> row's Type 3 value is maintained. A status-only change creates a version but does not touch
> `current_plan_tier` or `previous_plan_tier`.

Three independent mechanics operating on the same table, in the same build:

1. **Type 2** (versioning): a new row is inserted whenever `plan_tier` or `status` changes,
   exactly like `dim_title`.
2. **Type 1** (overwrite, broadcast across history): `email`, `display_name`, `country_code`,
   `acquisition_channel`, and `current_plan_tier` are mirrored onto *every* historical row of a
   subscriber whenever a newer value appears, not only onto the newest row. This is what makes
   `current_plan_tier` answerable with a plain filter regardless of which version row you land on.
3. **Type 3** (one step back): `previous_plan_tier` records the `plan_tier` value immediately
   before the most recent genuine plan change, and only that single step.

**Real SQL.** The Type 1 broadcast mechanic, `dim_subscriber.sql`'s `latest_event` CTE ("In a
full-refresh build the net effect of 'overwritten on every event, in event order' is just 'the
chronologically last event's value', broadcast to every version row"):

```sql
latest_event as (
    select
        subscriber_id,
        email,
        display_name,
        country_code,
        acquisition_channel,
        plan_tier as current_plan_tier
    from (
        select
            *,
            row_number() over (partition by subscriber_id order by changed_at desc) as _rn
        from filtered
    ) as ranked
    where _rn = 1
)
```

joined into every output row later (`inner join latest_event as le on v.subscriber_id =
le.subscriber_id`), which is what physically broadcasts one value across all of that subscriber's
version rows.

The Type 3 mechanic, `plan_prev_by_segment`, deriving `previous_plan_tier` only from genuine
`plan_tier` changes (a `plan_segment` counter that increments only when `plan_tier` itself
changes, distinct from `version_group` which also increments on a status-only change):

```sql
plan_prev_by_segment as (
    select
        subscriber_id,
        plan_segment,
        plan_tier,
        lag(plan_tier) over (partition by subscriber_id order by plan_segment)
            as previous_plan_tier
    from (
        select distinct subscriber_id, plan_segment, plan_tier
        from versions_keyed
    ) as distinct_segments
)
```

The version-boundary and `row_hash` mechanics are the same construction as `dim_title`'s (a
`row_hash` over the Type 2 tracked columns, here `(plan_tier, status)`, gates whether a new
version opens), reused directly:

```sql
hashed as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['plan_tier', 'status']) }} as row_hash
    from filtered
)
```

**The tie-break rule**, identical in shape to `dim_title`'s, implemented in `dim_subscriber.sql`'s
`same_instant_deduped` CTE (`row_number() over (partition by subscriber_id, changed_at order by
_batch_id desc, change_event_id desc)`, keeping only `_tie_rank = 1`), per modeling.md's shared
rule quoted in full in the Type 2 section above. The `row_hash` change-detection mechanism that
determines which events collapse into the same version is the same mechanism as `dim_title`'s and
is covered in full in the separate theory doc on change detection.

**Storage growth: identical formula to Type 2, real dim_subscriber ratio.** Type 6's versioned
columns grow storage by the exact same formula as pure Type 2 (`R = n + sum(c_i)`), since the
extra Type 1 and Type 3 mechanics only change what gets written *into* each row, not how many rows
get written. Real data, `.notes/decisions.md`, 2026-08-04: "Real build: 125,616 rows (125,615 real
versioned rows plus the one unknown member... 50,001 distinct subscriber_id values including
'-1'." With `n = 50,000` real subscribers and `R = 125,615` real versioned rows, the multiplier is
`R / n = 125,615 / 50,000 = 2.5123` rows per subscriber, i.e. an average of `1.5123` tracked
`(plan_tier, status)` changes per subscriber since the dimension's inception, against
`dim_title`'s `1.8512 / 0.8512` figures and `dim_plan`'s fixed `1.0`. The comparison across all
three real dimensions makes the growth-versus-change-frequency relationship concrete: `dim_plan`
(Type 3, no tracked-history columns versioned) never moves off `1.0` no matter how often prices
change; `dim_title` (Type 2, seven tracked columns) sits at `1.8512`; `dim_subscriber` (Type 6, two
tracked columns but higher change frequency per entity in the underlying event stream, 199,928
events across 50,000 subscribers) sits highest at `2.5123`.

**Query complexity: the actual tradeoff Type 6 buys.** A Type 6 consumer chooses between two
different costs depending on the question:

*Query A, current-state question ("how many subscribers are on each plan tier right now"), the
cheap path, a plain filter with no interval logic at all:*

```sql
select current_plan_tier, count(*) as subscriber_count
from dim_subscriber
where is_current and subscriber_id <> '-1'
group by current_plan_tier;
```

This works because `current_plan_tier` is the Type 1 mirror broadcast onto every row, so filtering
to `is_current` (one row per subscriber) and reading `current_plan_tier` directly answers "right
now" with zero join complexity, identical in shape to a Type 1 query.

*Query B, point-in-time correctness question ("what plan tier was each subscriber actually on
during each playback session"), the full path, the interval join against the Type 2 tracked
column:*

```sql
from silver_playback_sessions as f
left join dim_subscriber as ds
  on ds.subscriber_id = f.subscriber_id
  and f.session_started_at >= ds.effective_from
  and f.session_started_at <  ds.effective_to
```

selecting `ds.plan_tier` (not `ds.current_plan_tier`), the value genuinely in effect at
`session_started_at`, which can differ from `current_plan_tier` for any subscriber who has changed
plans since that session. In practice this second join has already been paid for by the time a
consumer queries the fact table: `fct_playback_events.subscriber_sk` is written by this exact
interval join at load time (modeling.md's point-in-time join rule), so a fact-to-dimension join on
`subscriber_sk` already resolves to the correct historical version with no interval predicate the
consumer has to write themselves. The interval predicate above is only needed for an ad hoc
question asked directly against `dim_subscriber` at an arbitrary instant with no existing fact row
to anchor to.

That is the concrete tradeoff Type 6 buys over a pure Type 2 dimension: the cheap, current-state
question (Query A) does not pay the interval-join cost that a pure Type 2 dimension would force on
every query, current or historical alike, while the point-in-time question (Query B) still has the
same full correctness available whenever it is genuinely needed.

**When it fails, and the counter-indications.** Type 6 costs more to build and reason about than
any of its three constituents alone: a builder maintaining it has to get all three mechanics right
simultaneously and keep straight which columns belong to which mechanic (modeling.md's exclusion
list for `row_hash` exists specifically to prevent Type 1 and Type 3 columns from spuriously
triggering new versions, "including [Type 1 columns] would spuriously version the entire chain on
each overwrite"). It is the wrong choice when a dimension genuinely only needs one of the three
behaviors: `dim_device` gains nothing from Type 6's extra machinery since it has no attribute that
needs point-in-time history at all, and `dim_plan` gains nothing from full Type 2 versioning of
attributes that only need one step of before/after comparison.

**Verifying it.** Real tests from `_dim_subscriber.yml`, structurally identical to `dim_title`'s
(the same interval and current-row-uniqueness pattern applies, since the Type 2 mechanic
underneath Type 6 is the same one):

```yaml
tests:
  - dbt_utils.mutually_exclusive_ranges:
      lower_bound_column: effective_from
      upper_bound_column: effective_to
      partition_by: subscriber_id
      gaps: not_allowed
      zero_length_range_allowed: false
  - dbt_utils.unique_combination_of_columns:
      combination_of_columns: [subscriber_id, effective_from]
  - dbt_utils.unique_combination_of_columns:
      combination_of_columns: [subscriber_id]
      config:
        where: "is_current"
  - dbt_expectations.expect_column_distinct_count_to_equal_other_table:
      column_name: subscriber_id
      compare_model: ref('dim_subscriber')
      compare_column_name: subscriber_id
      compare_row_condition: "is_current"
```

Idempotency, `.notes/decisions.md`, 2026-08-04: three separate full-rebuild runs of `dbt build
--select dim_subscriber` against unchanged silver input produced "an identical order-independent
row checksum (md5 of every column except loaded_at...)." A direct query to confirm the Type 1
broadcast mechanic specifically (every historical row of a subscriber should show the *same*
`current_plan_tier`, even rows whose own `plan_tier` differs):

```sql
select subscriber_id, count(distinct current_plan_tier) as distinct_mirror_values
from dim_subscriber
where subscriber_id <> '-1'
group by subscriber_id
having count(distinct current_plan_tier) > 1;
-- expect zero rows: current_plan_tier must be identical across every version of one subscriber
```

## Type 7: dual Type 1 and Type 2 keys on the fact

**The problem it solves.** A fact table sometimes needs to support both "as it was" and "as it is
now" queries against the same dimension reference without re-deriving either one at query time.
Type 7 solves this by having the fact carry two foreign keys to the same dimension simultaneously:
a durable key that always resolves to the entity's *current* row (functionally a Type 1 reference),
and the dated, version-specific Type 2 surrogate key that pins the exact historical version. A
consumer picks whichever key matches the question.

**Why this project does not use it.** `dim_subscriber`'s Type 6 hybrid already delivers both
capabilities Type 7 exists to provide, without touching the fact table's grain or adding a second
foreign key column. A Type 7 design would have `fct_playback_events` carry both a Type 2
`subscriber_sk` (pinned to the version at `session_started_at`) and a separate "current" key
resolving to whichever row currently has `is_current = true` for that subscriber; that second key
would cost another `VARCHAR(32)` column repeated across `fct_playback_events`'s ~120M rows. Type 6
gets the same "ask for current state cheaply" property for free by mirroring `current_plan_tier`
directly onto every historical dimension row instead: a consumer joins the fact to the dimension
once, on the single `subscriber_sk` the fact already carries, and can read either `plan_tier`
(the pinned, point-in-time value) or `current_plan_tier` (the Type 1 mirror, always current) off
the very same joined row. Adding Type 7's second fact-side key would duplicate a capability
`dim_subscriber` already provides at the dimension level, at real storage cost and no functional
gain over what is already there.

## Summary

| type | row count vs. entity count | needs interval join? | this project's example |
|------|----------------------------|-----------------------|-------------------------|
| 0    | 1:1, value frozen at first write | no | not implemented (closest analog: `dim_subscriber.signup_date`, a static column, not a dimension type) |
| 1    | 1:1, always | no | `dim_device`, 3,001 rows / 3,000 devices |
| 2    | grows with change frequency, `1 + mean(c_i)` | yes, always | `dim_title`, 9,256 / 5,000 = 1.8512 |
| 3    | 1:1, always | no (one prior value inline) | `dim_plan`, 31 / 30 = 1.0 |
| 4    | split: base 1:1, history table grows | only against the history table | not implemented |
| 5    | split: base 1:1, mini-dim grows | only against the mini-dim | not implemented |
| 6    | grows with change frequency on tracked columns only | only for point-in-time questions; current-state is a plain filter | `dim_subscriber`, 125,615 / 50,000 = 2.5123 |
| 7    | 1:1 base plus per-version history, two fact-side keys | one key needs it, one does not | not implemented (Type 6 subsumes it here) |
