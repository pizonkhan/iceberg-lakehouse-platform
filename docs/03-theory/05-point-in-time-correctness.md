# Point-in-time correctness

## The problem, stated precisely

A fact table rarely has one relationship to a dimension. It has one relationship to a dimension
*as that dimension existed at the moment the fact happened*. A playback session that occurred in
March, for a subscriber who was on the standard plan in March and upgraded to premium in July,
belongs to the standard-plan version of that subscriber. If the fact resolves to whatever
`dim_subscriber` looks like today (or at load time, or at query time), March's session gets
attributed to premium, because premium is the subscriber's current state. The row still writes.
The join still succeeds. The number still looks plausible. It is simply wrong.

This is, by a wide margin, the most common silent bug class in dimensional modeling, and the
reason it is common is the same reason it is dangerous: it is silent. A join miss produces a
NULL or an obvious gap, something a reviewer notices. A point-in-time miss produces a value, a
real plan tier, a real status, attached to the wrong historical instant. Nothing crashes, no test
written against row counts or referential integrity catches it, and the aggregate numbers built
on top (revenue by plan tier over time, churn cohorts, funnel conversion by acquisition channel)
all shift by some amount that is never large enough to look obviously broken. The failure mode is
not "the query errors." It is "the query answers a different question than the one asked, using
today's dimension state as if it were history," and the person reading the dashboard has no way
to tell the difference from the output alone.

This document covers the mechanism this project uses to make that impossible: resolving every
fact-to-SCD-dimension foreign key against the version of the dimension that was current at the
fact's own event time, using a half-open interval-overlap join. It also covers a real timestamp
precision bug this project hit during actual builds, one that made the mechanism fail almost
completely on one fact table until the root cause was found and a documented mitigation applied.

## The mechanism, from first principles

A Type 2 (or Type 6 hybrid) slowly changing dimension does not store one row per entity. It stores
one row per *version* of an entity, where a version is the state that held between two points in
time. `dim_subscriber` in this project is Type 6: it tracks `plan_tier` and `status` as versioned
(Type 2) attributes, while `email`, `display_name`, and `current_plan_tier` are Type 1 overwrites
mirrored across every historical row, and `previous_plan_tier` is a Type 3 one-step-back column.
`dim_title` is a plainer Type 2: any tracked metadata or rating change opens a new version.

Every version carries two timestamp columns, `effective_from` and `effective_to`, which together
define the half-open span during which that version was the true state of the entity:

```
[effective_from, effective_to)
```

A fact resolves its dimension foreign key by finding the one version whose span contains the
fact's own event timestamp, not the dimension's current row, not the row that existed at whatever
wall-clock instant the fact happened to be loaded. `.notes/modeling.md`'s "Point-in-time join
rule" names the event-time column per fact explicitly, because getting this column choice right
is exactly as load-bearing as getting the join predicate right:

- `fct_playback_events`: `session_started_at`
- `fct_billing_transactions`: `transaction_posted_at`
- `fct_watchlist_adds`: `added_at`
- `fct_signup_funnel`: `registered_at`, pinned once at registration and never repointed by later
  milestone updates
- `fct_daily_subscription_snapshot`: the last microsecond of the snapshot day

## The math: why half-open, formally

Let a dimension's version history for one natural key be an ordered sequence of intervals
`I_1, I_2, ..., I_n`, where version `v`'s interval is:

```
I_v = [effective_from_v, effective_to_v)
```

A fact with event timestamp `t` resolves to version `v` exactly when the predicate

```
effective_from_v <= t  AND  t < effective_to_v
```

holds. This project's SCD dimensions additionally guarantee, by construction and by a data test
(`dbt_utils.mutually_exclusive_ranges`, `gaps: not_allowed`, `zero_length_range_allowed: false`,
partitioned by the natural key), that consecutive versions of one entity are *contiguous and
non-overlapping*: `effective_to_v = effective_from_{v+1}` for every `v` but the last, and the
final version's `effective_to` is the literal high date `9999-12-31 23:59:59.999999`, never NULL.
Contiguity plus non-overlap plus half-open boundaries together partition the entire real time
line into disjoint spans, one per version, which is what makes the predicate above match *at
most one* version for any `t`. No `COALESCE` on `effective_to` is needed for the join itself
(only for the final key selection when no version matches at all, covered below), because there
is never a gap for a fact to fall into.

The half-open choice is not cosmetic. Consider the alternative, a closed interval
`[effective_from_v, effective_to_v]`. At the exact instant a subscriber's plan changes, the old
version's `effective_to` and the new version's `effective_from` are the identical timestamp (this
project enforces that identity directly: a version's `effective_to` equals the next version's
`effective_from` with no gap). A closed-closed predicate would match *both* versions at that
boundary instant, an ambiguous join that either silently picks one arbitrarily (nondeterministic
across query plans) or fails the "matches at most one row" invariant the incremental MERGE and
every downstream analytic query rely on. An open-open interval has the opposite problem: it
matches *neither* version at the boundary instant, silently dropping a fact that happens to
timestamp exactly on a change. Half-open, closed on the start and open on the end, is the only
choice under which the boundary instant belongs to exactly one version, and it is the version that
makes semantic sense: the instant a change becomes effective is the instant the *new* state holds,
so a fact timestamped at that exact instant should resolve to the new version, which is precisely
what `t >= effective_from_v` (new version) and `t < effective_to_v` (excludes the old version at
its own boundary) produce together.

## This project's actual literal predicate

`.notes/modeling.md`'s "Point-in-time join rule" gives the literal SQL for `fct_playback_events`
resolving `dim_subscriber`:

```sql
from silver_playback_sessions as f
left join dim_subscriber as ds
  on ds.subscriber_id = f.subscriber_id
  and f.session_started_at >= ds.effective_from
  and f.session_started_at <  ds.effective_to
```

then `coalesce(ds.subscriber_sk, md5('-1'))` as the written foreign key. The coalesce is a guard,
not a fallback path: the join is a `LEFT JOIN` because a match can legitimately fail (a residual
case covered below), and a genuine miss resolves to the unknown member rather than dropping the
row, matching this project's rule that every fact FK column is `NOT NULL`.

The real built model, `transform/lakehouse/models/marts/facts/fct_playback_events.sql`, implements
this exactly:

```sql
subscriber_resolved as (

    select
        p.*,
        ds.subscriber_sk
    from playback as p
    left join {{ ref('dim_subscriber') }} as ds
        on ds.subscriber_id = p.subscriber_id
        and p.session_started_at >= ds.effective_from
        and p.session_started_at < ds.effective_to

),
```

with `title_resolved` running the identical predicate shape against `dim_title` on the same
`session_started_at`, and the final select writing
`coalesce(subscriber_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }})` as the FK. This is not
an illustrative simplification of the modeling.md text; it is the equality-plus-half-open-range
predicate, unmodified, run against a 119,640,099-row fact.

## Worked example: the one-second lower-bound widening

Real builds surfaced a genuine precision mismatch that the literal predicate above could not
absorb on one fact table, and the story is worth telling in full because it is exactly the kind
of bug this document exists to warn about: correct logic, wrong assumption about the data feeding
it.

**Root cause.** `generation/playback.py` and `generation/billing.py`, the vectorized numpy
generators responsible for the largest fact volumes, store event timestamps as `datetime64[s]`,
whole-second resolution, for performance. `generation/subscribers.py` and `generation/titles.py`,
small enough to be written as plain per-entity Python loops, kept true microsecond resolution.
The effect: `dim_subscriber.effective_from` and `dim_title.effective_from` carry real
microseconds, but fact event timestamps from the vectorized generators land exactly on the second,
every time. This is a generation-time characteristic present all the way from bronze, not a
silver bug introduced downstream (confirmed directly against bronze data before concluding
anything about silver).

Truncation error only matters when a fact's event time and a dimension version's `effective_from`
are supposed to be near-simultaneous, since whole-second rounding can then push the fact just
before the version boundary it actually belongs to. For most facts this is rare: playback and
billing events happen well after signup, so a session or a charge landing within a second of a
plan-tier change is a coincidence. `fct_signup_funnel` is the opposite case. Its `registered_at`
is *the same event that creates the subscriber's first `dim_subscriber` version*, so the two
timestamps are supposed to be near-identical by design, and whole-second truncation crosses that
boundary on almost every row.

**Measured before the fix.** Applying the literal, unwidened predicate from modeling.md against
the real built data: `dim_subscriber.effective_from` for a subscriber's first version is always at
or after the truncated `registered_at`, by less than one second, measured directly across all
62,976 registered rows as a 0 to 999 millisecond gap. The literal predicate resolved only 75 of
those 62,976 rows, 0.12 percent. Every other registered attempt would have fallen back to the
unknown member, defeating the entire stated purpose of pinning `subscriber_sk` at registration.

**The mitigation, and why it is safe.** `.notes/modeling.md` records the standardized fix: widen
only the *lower* bound of the interval predicate by one second, the maximum possible truncation
error given whole-second source precision:

```
f.event_time + interval '1' second >= ds.effective_from
and f.event_time < ds.effective_to
```

The upper bound is untouched, because it is being compared against the version's own true end
instant, not a truncated one, and that comparison already holds correctly. Widening only the side
that is actually mismeasured is what keeps the fix from introducing new ambiguity: it does not
risk crossing into an earlier version's territory, because dimension versions are built from real
change timestamps that are themselves at least one full second apart in every case this project
verified before relying on the mitigation.

The real model, `transform/lakehouse/models/marts/facts/fct_signup_funnel.sql`, writes the
algebraically equivalent form (subtracting one second from `effective_from` rather than adding it
to the event time, same predicate, different placement of the constant), plus a determinism guard
in case a future data refresh ever produces two version boundaries within a second of each other:

```sql
subscriber_candidates as (

    select
        d.signup_id,
        ds.subscriber_sk,
        row_number() over (
            partition by d.signup_id order by ds.effective_from desc
        ) as _rn
    from date_keyed as d
    inner join {{ ref('dim_subscriber') }} as ds
        on ds.subscriber_id = d.subscriber_id
        and d.registered_at >= ds.effective_from - interval '1' second
        and d.registered_at < ds.effective_to
    where d.registered_at is not null

),
```

**Measured after the fix.** All 62,976 of 62,976 registered rows resolve, with zero ambiguity
(verified: no `signup_id` produces more than one candidate row under the widened predicate). That
is the before/after this document set out to report: 75 of 62,976 (0.12 percent) resolving under
the literal predicate, 62,976 of 62,976 (100 percent) resolving after the one-second widening, on
the same real dataset, same build.

`fct_watchlist_adds` needed and received the identical widening (its `added_at` is also
whole-second only, 0 of 750,000 rows carry a sub-second component): the exact predicate missed
`subscriber_sk` on 82 of 750,000 rows, the widened predicate misses on 0 of 750,000, with the
minimum real gap between two consecutive `dim_subscriber` versions of the same subscriber (221,062
milliseconds) and `dim_title` versions (357,538,399 milliseconds) both far past the one-second
widening window, so no ambiguity was introduced there either. `fct_playback_events` and
`fct_billing_transactions`, by contrast, did **not** need the widening: their event columns are
also whole-second, but because playback and billing activity rarely lands within a second of a
version change, the unwidened predicate already resolves 99.991 percent of playback rows (all but
11,193 of 119,640,099) against `subscriber_sk`, and the residual miss is absorbed correctly by the
unknown-member coalesce rather than defeating the join's purpose the way it did for signup funnel.

## Why the naive alternative is worse than an error

A naive "join to the current version" implementation, `dim_subscriber` filtered to
`is_current = true` with no interval predicate at all, would not have failed on any of this data.
It would have returned a `subscriber_sk` for every fact row, a plausible plan tier and status for
every one of them, and passed every referential-integrity and not-null test in the project. It
would simply have attributed every historical playback session, every historical billing
transaction, to whatever plan tier and status that subscriber holds *today*. A subscriber who
signed up on the basic plan in 2023 and upgraded to premium in 2026 would show their entire three
years of playback history as premium-tier activity. Revenue-by-tier trends, churn-by-cohort
analysis, and funnel conversion segmented by plan would all be systematically wrong in a way that
looks internally consistent, because every number involved is a real number from a real
dimension row, just the wrong one for the instant being measured. This is strictly worse than a
query that errors: an error stops the analyst, a plausible wrong answer does not.

## When it fails, and the counter-indications

The mechanism has three known, verified failure modes in this project's real data, each diagnosed
to root cause rather than dismissed:

1. **Sub-second same-instant ordering artifacts.** 11,193 of 119,640,099 `fct_playback_events`
   rows (0.009 percent), across only 23 distinct subscribers, fail the `subscriber_sk` interval
   join even after accounting for the whole-second truncation pattern above, because their
   `session_started_at` lands a few dozen microseconds to just under one second *before* that
   subscriber's own earliest `dim_subscriber` version begins (one sampled case:
   `sub_049413`'s unresolved rows are all timestamped `2024-04-23 06:57:50.000000`, the matching
   dimension version starts `2024-04-23 06:57:50.060244`, 60.244 milliseconds later). This is a
   source-side precision/ordering artifact on a handful of subscribers, not a missing dimension
   row and not a load defect; it correctly falls back to the unknown member per the coalesce
   guard, which is the intended behavior for a genuine, rare miss rather than something to
   silently suppress.

2. **A dimension whose own version timing is wrong.** `fct_playback_events.title_sk` fails the
   interval join on 20,646,958 of 119,640,099 rows, 17.3 percent of the entire fact, even though
   every `title_id` referenced by playback genuinely exists in `dim_title`. This is not a
   precision-truncation problem: it is a generator defect. `dim_title` design assumed titles
   "arrive from a controlled catalog feed ahead of playback" (the reason `dim_title`, unlike
   `dim_subscriber`, has no late-arriving self-heal), but `generate_titles()` originally drew
   each title's `catalog_add_at` from the same shared time span playback and watchlist events draw
   their own timestamps from, with no dependency between a title's catalog date and the playback
   sampled against it. 4,833 of 5,000 titles ended up with playback sessions timestamped years
   before their own first catalog version (one sampled title, `tt_00221`, had its first tracked
   version dated the day before the generator's own "now" cutoff). The point-in-time join did
   exactly what it should: it refused to attribute those sessions to a title version that did not
   exist yet, and routed them to the unknown member instead of silently pretending a resolution
   held. That correctness is what surfaced the defect for root-causing and a real fix (a dedicated
   pre-launch catalog seed window, `CATALOG_SEED_LEAD_TIME`), rather than letting a naive "always
   resolve to something" join hide it. Verified after the fix: 0 of 119,640,099 rows miss
   `title_sk`, down from 20,646,958. This is the clearest demonstration in this project that a
   strict point-in-time predicate is a detector of upstream defects, not just a correctness
   mechanism for well-formed data; a looser join (current-version, or any join without the
   interval predicate) would have hidden this bug completely rather than exposing it.

3. **A dimension-side truncation mismatch too large for the standard one-second widening.** If a
   future fact's event column were truncated to a coarser grain than whole seconds (minute-level,
   for instance), or if two versions of the same natural key were ever produced less than one
   second apart, the one-second widening would either under-correct (leaving a real gap) or
   over-correct (matching the wrong, earlier version). Both are things this project checks before
   relying on the mitigation for any given fact, not assumptions taken on faith: `fct_watchlist_adds`
   confirmed a 221,062 millisecond minimum real version gap for `dim_subscriber` before adopting
   the widening, precisely so the mitigation could not silently cross into ambiguous territory.

## How to verify this is actually working

**Data tests on the dimension side**, run on every SCD dim via `dbt test`, confirm the invariant
the whole predicate depends on:

```yaml
tests:
  - dbt_utils.mutually_exclusive_ranges:
      lower_bound_column: effective_from
      upper_bound_column: effective_to
      partition_by: subscriber_id
      gaps: not_allowed
      zero_length_range_allowed: false
```

from `transform/lakehouse/models/marts/dimensions/_dim_subscriber.yml`. This is the test that
proves versions are contiguous and non-overlapping, the precondition for "the interval predicate
matches at most one row."

**A direct integration test against the real built tables**,
`tests/integration/test_point_in_time_join.py`, is this project's strongest proof, because it
does not trust its own fixture's recorded boundaries; it recomputes ground truth fresh against
`dim_subscriber` for every check:

```python
expected as (
    select m.constructed_id, m.subscriber_id, d.subscriber_sk as expected_sk
    from manifest m
    join iceberg.dev_dimensions.dim_subscriber d
        on d.subscriber_id = m.subscriber_id
        and m.event_timestamp >= d.effective_from
        and m.event_timestamp < d.effective_to
),
```

Its fixture, `generation/output/_pathology_manifest/midstream_join_targets.csv`, contains 300
playback and 100 billing rows deliberately constructed with an event timestamp inside an *older*
version's gap while the same subscriber also has a newer version, exactly the scenario where a
naive current-version join and a correct point-in-time join produce different answers. The suite
does not stop at proving the correct answer is returned
(`test_playback_resolves_point_in_time_version_not_current`); it separately proves the check has
teeth (`test_playback_point_in_time_resolution_has_teeth`) by asserting the resolved key actually
differs from the current version's key for every genuinely midstream row, so a regression that
silently resolved to "current version" instead of "point-in-time version" would fail loudly
rather than passing by coincidence.

**A direct query against the real data**, the same shape used to produce the numbers in this
document, is the fastest manual check:

```sql
select
    count(*) as registered_rows,
    count(*) filter (where subscriber_sk <> (
        select subscriber_sk from iceberg.dev_dimensions.dim_subscriber where subscriber_id = '-1'
    )) as resolved_rows
from iceberg.dev_facts.fct_signup_funnel
where registered_at is not null;
```

against the real table returns `registered_rows = 62976`, `resolved_rows = 62976`, matching the
after-fix figure above exactly.
