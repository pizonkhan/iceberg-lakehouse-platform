# Late-arriving data

This document covers two related but distinct problems: a **late-arriving dimension member**, a
fact referencing an entity that has not yet appeared in its dimension feed, and a **late-arriving
fact**, an event that arrives after the dimension it should join to has already changed versions.
They look similar (both involve something showing up "late" relative to something else) and they
are solved by two different mechanisms in this project. Conflating them is a common source of
confusion, so this document keeps them separate throughout.

## Part one: late-arriving dimension members

### The problem, stated precisely

`dim_subscriber` is built before any fact that references it. But the source feeds are not
guaranteed to arrive in dependency order: a playback session, a billing charge, or a watchlist add
can reference a `subscriber_id` that has not yet produced a single row in
`silver_subscriber_events`, the change-event stream `dim_subscriber` is built from. If the load
does nothing special about this, one of two things happens, and both are wrong.

**Option A: skip resolution, leave the fact unlinked or reject it.** The fact either never lands
in gold, or lands with no usable subscriber key at all. Either way, that subscriber's activity is
permanently invisible to any subscriber-level rollup. Worse, if the real profile data does arrive
later, there is no way to retroactively repair a row that was silently dropped, unless the load
also reprocesses history from scratch.

**Option B: fall back to the unknown member.** The fact writes with `subscriber_sk` pointing at
the dimension's single unknown-member row (`md5('-1')`, natural key `'-1'`, every descriptive
attribute `'Unknown'`). This does not lose the row, but it permanently and irreversibly merges
this subscriber's activity into the generic unknown bucket alongside every other subscriber that
could never be resolved for any reason. When the real subscriber profile shows up later, there is
no way to reattach the fact to it: the unknown member is shared by every unresolvable fact across
the whole table, so there is no way to distinguish "this row was really this specific
not-yet-arrived subscriber" from "this row was genuinely unresolvable." The fact is correct
(a real event happened) but permanently misattributed, which is the same silent-wrongness problem
covered in the point-in-time correctness document, just triggered by dimension timing instead of
version timing.

### The mechanism: the inferred member pattern

This project's actual answer is neither of those. `dim_subscriber` detects, on every build, every
`subscriber_id` referenced by a fact-side silver table (`silver_playback_sessions`,
`silver_billing_ledger`, `silver_watchlist_adds`) that does not appear anywhere in
`silver_subscriber_events`, and synthesizes a real dimension row for it before any fact tries to
resolve against it:

```sql
-- late-arriving subscriber self-heal (modeling.md, "Late-arriving
-- subscriber handling"). Because this dimension must be built before any
-- fact, a fact can never write into it; instead this model detects, on
-- every build, any subscriber_id referenced by a fact-side silver table
-- that does not appear anywhere in silver_subscriber_events, and
-- synthesizes the inferred row itself, so the point-in-time interval
-- join in every downstream fact always resolves.
fact_subscriber_ids as (

    select subscriber_id from {{ ref('silver_playback_sessions') }}
    union
    select subscriber_id from {{ ref('silver_billing_ledger') }}
    union
    select subscriber_id from {{ ref('silver_watchlist_adds') }}

),

late_arriving_ids as (

    select distinct f.subscriber_id
    from fact_subscriber_ids as f
    left join (select distinct subscriber_id from {{ ref('silver_subscriber_events') }}) as e
        on f.subscriber_id = e.subscriber_id
    where e.subscriber_id is null

),
```

Every id in `late_arriving_ids` gets a synthesized row, `is_inferred = true`, with `plan_tier` and
`status` both set to the literal value `'unknown'` and, critically, `effective_from` pinned to a
sentinel date far in the past:

```sql
inferred_base as (

    select
        subscriber_id,
        timestamp '1900-01-01 00:00:00.000000' as effective_from,
        'unknown' as plan_tier,
        'unknown' as status
    from late_arriving_ids

),

inferred_rows as (

    select
        {{ dbt_utils.generate_surrogate_key(['subscriber_id', 'effective_from']) }}
            as subscriber_sk,
        subscriber_id,
        cast(null as varchar) as email,
        cast(null as varchar) as display_name,
        cast(null as varchar) as country_code,
        cast(null as varchar) as acquisition_channel,
        cast(null as date) as signup_date,
        plan_tier,
        status,
        'unknown' as current_plan_tier,
        cast(null as varchar) as previous_plan_tier,
        cast(null as integer) as churn_date_key,
        effective_from,
        {{ high_date }} as effective_to,
        true as is_current,
        1 as scd_version,
        {{ dbt_utils.generate_surrogate_key(['plan_tier', 'status']) }} as row_hash,
        true as is_inferred
    from inferred_base

),
```

from `transform/lakehouse/models/marts/dimensions/dim_subscriber.sql`.

### The math, and why the sentinel date is not arbitrary

This is a direct application of the half-open interval predicate covered in
[05-point-in-time-correctness.md](05-point-in-time-correctness.md). Every real fact event
timestamp `t` produced by this platform satisfies `t >= 2023-01-01` (the platform's launch date,
`dim_date`'s own floor). An inferred row's interval is `[1900-01-01, 9999-12-31 23:59:59.999999)`.
For the predicate `effective_from <= t < effective_to` to match, it is sufficient that
`effective_from` be less than or equal to the *earliest possible* real event timestamp in the
system, and `1900-01-01` is chosen specifically to be far earlier than `2023-01-01` with margin to
spare, so this holds unconditionally for every event this platform can ever produce, not just the
events observed so far. That is what makes the inferred row's interval a strict superset of every
real event's arrival window: it is not tuned to today's data, it is a structural guarantee. Any
real event timestamp, no matter how early relative to when the subscriber's profile data
eventually arrives, falls inside the inferred row's interval and resolves through the exact same
join predicate every other fact uses, with no special-case code required in the fact models.

### Worked example, and this project's real numbers

`generation/output/_pathology_manifest/late_arriving_subscribers.csv` lists 200 `subscriber_id`
values the generator deliberately made appear in a fact-side event stream without a corresponding
`subscriber_events` record at generation time (pathology 1 in this project's synthetic pathology
suite). This is the fixture that exercises the mechanism.

In the real built table today, the inferred-row path is a genuine no-op: `dim_subscriber`'s
2026-08-04 build entry in `.notes/decisions.md` records **0 late-arriving inferred rows**, out of
125,616 total dimension rows (125,615 real versioned rows plus the one unknown member). Every one
of the 200 manifest subscriber ids already has a real `silver_subscriber_events` history by the
time the dimension is built, so `late_arriving_ids` evaluates to an empty set on every current run.

This is worth being honest about rather than glossing over: **the mechanism is correctly
implemented, tested, and connected end to end, but currently unexercised in production data.**
The reason is structural, not a gap in the pathology design. This project's bronze layer is a
single full load (`.notes/decisions.md`'s 2026-08-04 ingestion entry: one `_batch_id` per table,
confirming one ingestion run produced each table's rows), and `dim_subscriber` is built
full-refresh, reprocessing all of `silver_subscriber_events` from scratch on every run. The
generator constructs the late-arrival pathology by controlling *file write order* within a single
batch (subscriber events for these 200 ids are written to the physically last file,
`subscriber_events/part-00001-late-arrival.parquet`, while their fact-side events land in earlier
batches), which produces the intended effect on a genuinely incremental, multi-run pipeline where
facts can be ingested before a later dimension load catches up. On a single-load, full-refresh
pipeline, every source file, including the deliberately-last-written one, is fully present by the
time `dim_subscriber` builds, so there is no window in which a fact-referenced subscriber_id is
actually missing from the dimension's own source data. The pathology tests the *shape* of the
handling correctly; it does not, on this pipeline's current operating mode, produce a live
inferred row to observe. `.notes/modeling.md`'s own spec anticipates exactly this outcome: "the
moment real profile events exist for a subscriber_id, that id no longer appears in
`late_arriving_ids` at all."

### Backfill: how a stable key survives the transition from inferred to real

`.notes/modeling.md` describes the backfill mechanic for an incrementally-maintained dimension:
when real profile data for a previously-inferred subscriber arrives, the load matches on
`subscriber_id` where `is_inferred = true`, updates that row in place (Type 1 overwrite on every
descriptive attribute, including `plan_tier` and `status`, with `row_hash` recomputed), and sets
`is_inferred = false`, while leaving `subscriber_sk`, `effective_from`, and `scd_version`
untouched. This is the detail that matters most: because the surrogate key is a deterministic hash
of `(subscriber_id, effective_from)` and both of those inputs are frozen at the sentinel date, the
key never changes when the row transitions from inferred to real. Every fact that already resolved
to that `subscriber_sk` keeps pointing at a valid, now-enriched row, with no re-keying pass needed
anywhere downstream. The next genuine tracked change after backfill closes this row at the real
change timestamp and inserts `scd_version` 2 through the normal Type 2 path, exactly like any other
subscriber's second version.

Because this project's `dim_subscriber` is full-refresh rather than incremental, the literal
"update in place" step described above does not apply in this codebase as written: there is no
persisted prior gold state within a single full-refresh build to update. Instead, a subscriber_id
is only ever synthesized as inferred if it is completely absent from `silver_subscriber_events` at
build time; the instant real profile events exist for it, the id is excluded from
`late_arriving_ids` entirely and flows through the normal Type 2/6 versioning path with a real
`effective_from` at `scd_version` 1. This reaches the identical practical end state, a real,
versioned history that facts can pin to, without a separate in-place update step, because a full
refresh recomputes the whole table from current silver state every time rather than diffing
against its own prior output. The key-stability *property* the backfill mechanic exists to provide
is preserved; the *mechanism* that provides it is structurally different under full-refresh than
under an incremental merge, and this project's model comments and `.notes/decisions.md` record
that difference explicitly rather than let it read as an unexplained deviation from the documented
spec.

### The naive alternatives, and why they are wrong

Revisit the two options from the top of this document in light of the mechanism above:
**skip resolution** loses the fact from every subscriber-level rollup permanently, with no
avenue to recover once real data lands, because nothing was ever recorded that could later be
matched and repaired. **Force to the unknown member** loses the fact's identity into a shared
bucket with every other genuinely unresolvable row, and because the unknown member's key does not
encode which specific subscriber it stood in for, there is no way to later split that bucket back
apart when the real profile shows up. The inferred-member pattern avoids both failure modes by
giving the fact a real, stable, subscriber-specific key immediately, one that is corrected in
place rather than replaced when better data arrives.

### How to verify this is actually working

`tests/integration/test_late_arriving_dimension.py` is written specifically to remain meaningful
regardless of which state (inferred or already-backfilled) the real data happens to be in, since
the test suite cannot assume today's zero-inferred-rows state will hold forever:

```python
def test_late_arriving_subscribers_are_not_orphaned(
    trino_conn: Connection, late_arriving_ids: list[str]
) -> None:
    """Every manifest subscriber_id must exist in dim_subscriber, whether it
    landed there as a still-inferred row or was fully backfilled through
    the normal versioned path by build time."""
    present = scalar(
        trino_conn,
        f"""
        select count(distinct subscriber_id)
        from iceberg.dev_dimensions.dim_subscriber
        where subscriber_id in ({sql_in_list(late_arriving_ids)})
        """,
    )
    assert present == len(late_arriving_ids), ...
```

This is the test that matters most: it asserts the outcome (never orphaned) that holds true in
either state, rather than asserting a specific mechanism was exercised. A second test,
`test_any_remaining_inferred_rows_match_the_documented_shape`, validates the sentinel
`effective_from`, the exactly-one-inferred-row-per-id invariant, and the general row shape *if*
any inferred rows exist, but explicitly skips with a stated reason rather than silently passing
when the count is zero, exactly this project's current state:

```python
if not rows:
    pytest.skip(
        "no is_inferred=true rows remain for any manifest subscriber_id "
        "(fully backfilled in the current dataset); nothing to validate the shape of"
    )
```

A third test, `test_late_arriving_subscriber_facts_do_not_fall_back_to_unknown_member`, is the
strongest direct proof this mechanism is not silently degrading to the "force to unknown" failure
mode: it joins forward from silver (which still has `subscriber_id`) through each fact's own
degenerate-dimension id to gold's resolved `subscriber_sk`, and asserts that key is never the
unknown member's key for any row originally attributed to a late-arriving subscriber, across all
three fact-side silver tables that can legitimately reference one.

**A direct query** reproduces the current-state number cited above:

```sql
select count(*) from iceberg.dev_dimensions.dim_subscriber where is_inferred;
```

returns `0` against the real built table today, consistent with every manifest subscriber id
having already resolved through the normal versioned path by build time, not through the inferred
path, for the structural reason explained above.

## Part two: late-arriving facts

### The distinct problem

A late-arriving *fact* is the mirror image of a late-arriving dimension: not an entity missing
from the dimension, but an event whose timestamp falls inside a dimension version that has since
been superseded by a newer one, arriving into the fact table only after the dimension has already
moved on. `generation/playback.py`'s `_write_midstream_playback_targets` and
`generation/billing.py`'s `_write_midstream_billing_targets` construct this deliberately: for
subscribers with three or more change events, the generator records the first non-final gap
between two consecutive changes and constructs a fact row timestamped strictly inside that older
gap, while the subscriber's dimension history has already moved past it to a newer version by the
time the fact table is built. 300 playback rows and 100 billing rows are built this way
(`generation/output/_pathology_manifest/midstream_join_targets.csv`), specifically so a naive
"join to the newest version" implementation resolves the wrong row.

### Why the point-in-time join already handles this, and the one condition that matters

This is not a new mechanism. It is the direct consequence of the interval-overlap join covered in
[05-point-in-time-correctness.md](05-point-in-time-correctness.md): the join predicate never asks
"what is this subscriber's current dimension version," it asks "what dimension version's interval
contains this fact's own event timestamp." A late-arriving fact with an old event timestamp
resolves to the old version whose interval genuinely contains that timestamp, automatically,
regardless of when the fact itself physically arrives or how many newer dimension versions have
since been written. `tests/integration/test_point_in_time_join.py`'s
`test_playback_point_in_time_resolution_has_teeth` proves this directly against the real 300
constructed playback rows: it isolates the subset where the correct (older) version genuinely
differs from the subscriber's current version, and asserts every one of those rows resolved to
the older version, not the current one.

There is exactly one condition under which this automatic correctness breaks down, and it has
nothing to do with the join predicate itself: **the fact has to actually reach the join.** If an
incremental pipeline's watermark filters out the late-arriving fact before it is ever scanned
against the dimension, the interval-overlap predicate never gets a chance to run on that row at
all, correct or not. This project's own incremental design makes this connection explicit rather
than leaving it implicit. `ADR-004` records the reasoning directly:

> A watermark on event time would permanently and silently drop any row whose event time falls
> inside a range an earlier run already scanned, which is exactly what the out-of-order pathology
> produces on purpose: once a watermark on `session_started_at` has advanced past a straggler's
> true event time, that straggler would never be picked up by any later incremental run, no error,
> no trace.

Every incremental fact in this project (`fct_playback_events`, `fct_billing_transactions`,
`fct_watchlist_adds`, `fct_daily_subscription_snapshot`) is watermarked on bronze's `_ingested_at`,
never on the fact's own business event time, precisely so a late-arriving row with an old event
timestamp is picked up on whichever run's ingestion window it genuinely lands in, independent of
how old that event actually is:

```sql
where _ingested_at > (
    select coalesce(max(committed_at), timestamp '1900-01-01 00:00:00.000000 UTC')
    from {{ snapshots_relation }}
)
```

from `fct_playback_events.sql`. The connection is direct and worth stating plainly: the
point-in-time interval join is what makes a late-arriving fact resolve to the *correct historical
dimension version* once it is being processed at all; the ingestion-time watermark is what
guarantees the fact is not silently dropped from processing *before* it ever gets that chance. The
two mechanisms solve adjacent halves of the same problem, and a design that got either one wrong
(an event-time watermark, or a current-version join) would silently reintroduce exactly the
failure this document set out to prevent, just from a different direction.

### How to verify this is actually working

The same `tests/integration/test_point_in_time_join.py` suite covers this directly, since its
fixture is constructed specifically as a late-arriving-fact-against-a-historical-version scenario:
300 playback and 100 billing rows, each timestamped inside an older version's gap while a newer
version already exists. Its ground truth is deliberately not read from the manifest's own recorded
gap boundaries (those record raw pre-collapse event timestamps at generation time, which can
differ slightly from the actual built version boundaries after `dim_subscriber`'s SCD collapse
logic runs), but recomputed fresh against the real `dim_subscriber` table for every check, which is
the only definition of "correct" that actually matters:

```python
# ground truth: the dim_subscriber version whose real interval
# contains this fact's event timestamp, computed fresh here rather
# than trusted from the manifest's own recorded gap boundaries.
expected as (
    select m.constructed_id, m.subscriber_id, d.subscriber_sk as expected_sk
    from manifest m
    join iceberg.dev_dimensions.dim_subscriber d
        on d.subscriber_id = m.subscriber_id
        and m.event_timestamp >= d.effective_from
        and m.event_timestamp < d.effective_to
),
```

A run of `test_playback_point_in_time_resolution_has_teeth` and
`test_billing_point_in_time_resolution_has_teeth` against the real stack confirms both that every
constructed late-arriving row resolved (no orphans) and that none of them silently resolved to the
subscriber's current version instead of the older, correct one, which is the specific regression a
current-version join or a dropped-by-watermark row would each produce.
