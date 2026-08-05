# Change detection hashing

`dim_subscriber` and `dim_title` both need to answer the same question every time a new silver
event arrives for an entity they already track: does this event represent a genuine change to an
attribute this dimension is supposed to version, or is it a duplicate, a repeat, or a change to
something the dimension deliberately does not track? Answering that with a list of column-by-column
comparisons works but is verbose, easy to get subtly wrong (miss a column, and it silently stops
being tracked), and awkward to reuse across a lag comparison, an incremental diff, and a downstream
test. This project answers it with a single hash column, `row_hash`, computed the same way and with
the same tool as the surrogate key covered in `docs/03-theory/03-surrogate-key-strategies.md`. This
document is about that mechanism specifically: what goes into the hash, what must be kept out of
it, the null-handling detail that makes the whole thing work at all, and why the same collision math
that document already derived applies here without modification.

Column-level source of truth: `.notes/modeling.md`, "SCD mechanics shared by dim_subscriber and
dim_title". Real model code: `transform/lakehouse/models/marts/dimensions/dim_subscriber.sql`,
`dim_title.sql`.

## The problem it solves, stated precisely

`silver_subscriber_events` and `silver_title_events` are both change-event streams: one row per
observed profile or metadata event, not one row per dimension version. Turning that stream into an
SCD Type 2/6 history means deciding, for each event in a subscriber's or title's chronological
sequence, whether it opens a new version or is absorbed into the version already open. Modeling.md
states the rule directly:

> Change detection: `row_hash` = md5 over the tracked attribute set only (listed per table),
> components canonicalized the same way as surrogate keys ... A silver record whose row_hash
> equals the current row's hash produces no new version.

The property this buys is a single equality check standing in for "did any of the columns this
dimension is supposed to track actually change." `dim_subscriber` tracks two columns (`plan_tier`,
`status`) out of the fourteen substantive columns on the table; `dim_title` tracks seven out of
eight. Comparing `row_hash <> row_hash` once is equivalent to `plan_tier <> plan_tier OR status <>
status` for `dim_subscriber`, or the seven-way OR for `dim_title`, but it is one column, one
comparison, reusable everywhere a "did this version change" question needs asking: the lag
comparison that opens a new version, the incremental diff a future incremental SCD conversion would
need, and the not-null test that already ships on the column.

## The mechanism, from first principles

The hash is computed over the tracked columns only, using the identical macro call the surrogate
key uses (`dbt_utils.generate_surrogate_key`), and compared against the immediately preceding
event's hash for the same entity. `dim_title.sql` states the comparison logic directly in its own
comment, because the "compare to current row's hash" language in modeling.md and the "compare to
previous event's hash" shape the SQL actually uses are not literally the same operation, and the
model explains why they are equivalent:

```sql
-- change detection: a version boundary opens only where row_hash differs
-- from the immediately preceding event for the same title. comparing
-- each event only to its immediate predecessor in changed_at order is
-- equivalent to comparing to the currently open version's hash: every
-- event collapsed away here shares its predecessor's hash by
-- construction, so the comparison telescopes back to the last retained
-- version (see modeling.md, "change detection").
changes as (

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
        row_hash,
        lag(row_hash) over (
            partition by title_id order by changed_at
        ) as _prev_row_hash
    from tracked

),

versions as (

    select
        title_id,
        changed_at as effective_from,
        title_name,
        content_type,
        release_year,
        runtime_minutes,
        maturity_rating,
        original_language,
        is_original,
        row_hash
    from changes
    where _prev_row_hash is null or row_hash <> _prev_row_hash

)
```

Every event whose hash matches its immediate predecessor's hash is simply excluded from `versions`;
only the events that actually change something survive to become a new SCD version row. The
telescoping argument in the comment is the part worth making explicit: if event 2 has the same
hash as event 1, it is dropped, and event 3 is compared against event 2's hash, not against event
1's. But because event 2 was dropped for having the *same* hash as event 1, comparing event 3
against event 2's hash and comparing it against event 1's hash produce the identical answer, so the
chain of `lag()` comparisons never actually loses information even though it only ever looks one
step back.

`dim_subscriber.sql` needs a more elaborate version of the same idea, because it tracks two columns
that can each change independently (`plan_tier`, `status`) and has to derive three different
running counters from the same underlying hash comparison: a version boundary (either column
changed), a plan segment (only `plan_tier` changed, drives the Type 3 `previous_plan_tier`), and a
status segment (only `status` changed, drives the `churn_date_key` mirror). The hash itself is
computed once, then a cumulative sum over "did the hash change since the previous event"
materializes the version grouping:

```sql
hashed as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key(['plan_tier', 'status']) }} as row_hash
    from filtered

),

flagged as (

    select
        *,
        lag(row_hash) over (partition by subscriber_id order by changed_at)
            as _prev_row_hash,
        ...
    from hashed

),

grouped as (

    select
        *,
        sum(case when _prev_row_hash is null or row_hash <> _prev_row_hash then 1 else 0 end)
            over (
                partition by subscriber_id order by changed_at
                rows between unbounded preceding and current row
            ) as version_group,
        ...
    from flagged

)
```

`version_group` increments by one every time `row_hash` differs from the row immediately before it,
and stays flat across any run of consecutive events that hash identically. Grouping by
`(subscriber_id, version_group)` afterward and taking `min(changed_at)` as `effective_from` is what
collapses a run of unchanged-hash events into a single materialized version row.

## Why audit columns must be excluded

Modeling.md states the exclusion list directly, and it is worth reading as a single unit because
each entry is a distinct failure mode being ruled out, not one generic caution repeated five ways:

> Columns always excluded from row_hash, in every SCD dim, and why:
> - The surrogate key and natural key: identity, not an attribute; the natural key can never
>   change within its own history chain.
> - All SCD tracking columns and `is_inferred`: bookkeeping about versioning, not state.
> - All Type 1 columns: they are overwritten across every historical row, so including them would
>   spuriously version the entire chain on each overwrite.
> - All Type 3 columns: derived from the Type 2 change itself.
> - `loaded_at` and any bronze metadata (`_batch_id`, `_payload_hash`, `_source_file`,
>   `_ingested_at`): load mechanics, not state.

The `loaded_at` case is the most mechanically severe, and it is worth walking through concretely
rather than taking on faith, because both `dim_subscriber` and `dim_title` are full-refresh models:
every build recomputes `row_hash` for every raw silver event from scratch, and `loaded_at` is
written once, at the very end, as `cast(current_timestamp as timestamp(6))`, a single wall-clock
value applied uniformly to the whole build. If `loaded_at` (or `current_timestamp` itself) were
folded into the hash input for `row_hash`, one of two things would happen depending on exactly
where the timestamp got captured:

- If a fresh `current_timestamp` were read once per row during hash computation, no two rows in the
  same build would even reliably hash identically to each other for identical input, since a wide
  scan does not guarantee every row sees the same clock read.
- If a single `current_timestamp` were captured once for the whole build (the more likely mistake,
  matching how `loaded_at` is actually assigned today) and threaded into every row's hash input,
  every row in a given build would get a hash that is a function of *that build's* wall-clock
  moment, not of `plan_tier` and `status` alone. The immediate practical effect: **every rebuild
  would produce a different `row_hash` for every single row**, even though nothing about
  `plan_tier` or `status` changed. The `lag(row_hash)` comparison inside the same build would still
  work correctly within that one build's internal consistency (every event in that build shares the
  same contaminated timestamp component, so consecutive events with the same real `plan_tier`/`status`
  would still compare equal to each other), but any downstream consumer comparing `row_hash` across
  builds (a future incremental SCD conversion diffing an incoming row against this table's own
  previously materialized `row_hash`, which is exactly the diff mechanism modeling.md's own
  incremental-conversion section names as the reason SCD dimensions stayed full-refresh) would see
  every row's hash change on every rebuild and be unable to distinguish a real change from mere
  wall-clock noise. That is precisely the failure mode modeling.md is naming when it says
  `loaded_at` is "never used in any join or hash": the column exists specifically so that nothing
  downstream ever has to reason about build time as if it were part of an entity's state.

The Type 1 case is the same shape of bug wearing different clothes. `dim_subscriber`'s Type 1
mirrors (`email`, `display_name`, `country_code`, `acquisition_channel`, `current_plan_tier`) are
"overwritten across all rows of a subscriber whenever silver shows a new value," per the model's
own comment. If any of those columns were folded into `row_hash`, a subscriber updating their email
address, with `plan_tier` and `status` both unchanged, would spuriously open a brand-new SCD
version, exactly the outcome Type 1 columns exist to avoid (a Type 1 change overwrites history, it
does not create it). The exclusion list rules this class of bug out at the design level rather than
relying on every future builder to remember it by inspection.

## Null handling in concatenation

`dbt_utils.generate_surrogate_key`'s real implementation, already quoted in full in
`docs/03-theory/03-surrogate-key-strategies.md`, casts every component to `VARCHAR`, substitutes a
fixed placeholder string for a `NULL` component, and only then concatenates with a literal `'-'`
delimiter:

```sql
{%- do fields.append(
    "coalesce(cast(" ~ field ~ " as " ~ dbt.type_string() ~ "), '" ~ default_null_value  ~"')"
) -%}
```

where `default_null_value` is `'_dbt_utils_surrogate_key_null_'` by default. That `coalesce(...)`
wrapper is not incidental plumbing; it is the entire reason `row_hash` works at all for
`dim_title`. In most SQL engines, Trino included, the string concatenation operator propagates
`NULL`: `a || b` returns `NULL` the instant either operand is `NULL`, regardless of what the other
operand contains. A naive, hand-written hash expression that skipped the coalesce, something like:

```sql
-- what NOT to do:
md5(title_name || content_type || cast(release_year as varchar)
    || cast(runtime_minutes as varchar) || maturity_rating
    || original_language || cast(is_original as varchar))
```

would return `NULL` for the entire hash the moment any single tracked column is `NULL`, silently.
Not an error, not a warning: a `NULL` `row_hash` that then participates in every downstream `lag()`
comparison as `NULL`. Because SQL's `<>` operator also propagates `NULL` (`NULL <> anything`
evaluates to `NULL`, not `TRUE` or `FALSE`), a `NULL`-valued `row_hash` compared against another
`NULL`-valued `row_hash` (two consecutive events for the same entity, both hitting the same missing
column) is neither equal nor unequal by SQL's three-valued logic; it is unknown, and the `WHERE
_prev_row_hash IS NULL OR row_hash <> _prev_row_hash` guard this project's models actually use
would treat every such row as if `_prev_row_hash` were genuinely absent (the very first event),
opening a spurious new version on every single subsequent event for that entity, forever, for as
long as the null-valued tracked column stayed null. That is a real, load-bearing bug class, and it
is exactly the kind of thing that passes casual testing cleanly: any row whose tracked columns are
all non-null would hash and compare correctly, so the bug only shows up on the subset of rows that
happen to carry a null in a tracked column, which is precisely the subset least likely to be in
whatever sample a developer eyeballs first.

### The concrete, real case in this project: `dim_title.runtime_minutes`

This is not a hypothetical risk this project happens to avoid; it is a column this project's own
`row_hash` genuinely has to handle correctly, on real data, every build. `runtime_minutes` is one of
`dim_title`'s seven tracked columns, and it is nullable by contract: modeling.md declares it
"NULL for series" outright, and the column's schema description repeats it: "Runtime in minutes for
a movie, as of this version. Always null for series." Every series title in this project's
~5,000-title catalog therefore has `runtime_minutes = NULL` on every one of its versions, forever,
by design, not as an edge case. The real `tracked` CTE in `dim_title.sql` computes `row_hash` over
all seven columns, `runtime_minutes` included, through the macro:

```sql
{{ dbt_utils.generate_surrogate_key([
    'title_name',
    'content_type',
    'release_year',
    'runtime_minutes',
    'maturity_rating',
    'original_language',
    'is_original'
]) }} as row_hash
```

Worked through concretely for a series title (`content_type = 'series'`, `runtime_minutes = NULL`,
the other six columns real values): the macro's real compiled form coalesces the null component
before concatenating, so the input to `md5()` looks like `title_name-series-release_year-
_dbt_utils_surrogate_key_null_-maturity_rating-original_language-is_original` (dash-delimited, per
the macro's real `'-'` delimiter, matching the delimiter behavior `docs/03-theory/03-surrogate-key-
strategies.md` already confirmed against a live Trino query), rather than a `NULL` that swallows
every other real value in the row. Because the placeholder string is a fixed, deterministic
constant, every series version with the identical six real tracked values hashes identically to
every other, and a genuine change to any of those six values (or a transition to a non-null
`runtime_minutes`, which would itself imply a `content_type` change from `series` to `movie`)
produces a different hash, exactly the behavior movie titles get for free without ever touching the
null path. Change detection for series titles works precisely as reliably as it does for movies,
solely because the macro's coalesce runs before concatenation, not after. `dim_title.sql`'s own
unknown-member row makes the same null-safety visible from the other direction, explicitly casting
`NULL` values through the macro rather than passing bare literals:

```sql
{{ dbt_utils.generate_surrogate_key([
    "cast('Unknown' as varchar)",
    "cast('Unknown' as varchar)",
    "cast(null as integer)",
    "cast(null as integer)",
    "cast('Unknown' as varchar)",
    "cast('Unknown' as varchar)",
    "false"
]) }} as row_hash,
```

The two `cast(null as integer)` components (standing in for `release_year` and `runtime_minutes`)
go through the identical coalesce-to-placeholder path as any real series row's `runtime_minutes`
would, which is exactly why this row's `row_hash` is a real, non-null, deterministic value rather
than `NULL`.

## Collision analysis

`row_hash` is produced by the identical macro, over the identical 128-bit md5 keyspace, as the
surrogate key `docs/03-theory/03-surrogate-key-strategies.md` already derives collision bounds for.
There is no separate collision analysis to perform here; the birthday-bound argument that document
builds, `P ≈ n²/2N` for `n` uniformly-hashed inputs into a keyspace of size `N = 2^128`, applies to
any set of md5 outputs this project produces, regardless of what those outputs are used to
identify. Using identity (a surrogate key must never collide with a different row's key) and using
change detection (`row_hash` must never collide with the hash of a genuinely different attribute
state, or a real change silently looks like no change at all) are different consumers of the same
underlying guarantee, not different guarantees. The shape of the risk is identical: some number of
distinct inputs get mapped into a fixed keyspace, and the question is the probability that any two
of them land on the same output.

If anything, `row_hash`'s real-world exposure is more conservative than the surrogate key's,
not less. A surrogate key's `n` is the full row count of the dimension (every version across every
entity draws from the same keyspace and any pair of them colliding would be a real problem). A
`row_hash` collision only matters when it happens **within one entity's own chronological chain**:
two different attribute states for the same `subscriber_id` or `title_id` landing on the same hash
would cause a real change to be missed for that one entity, but a `row_hash` collision between two
unrelated subscribers' versions has no consequence at all, since `row_hash` is never compared across
entities, only along one entity's own `lag()` chain. The relevant `n` for that failure mode is
therefore the number of distinct attribute-state versions one entity can accumulate over its own
history, typically single digits per subscriber and at most a handful per title, not the full table
row count. Reusing `docs/03-theory/03-surrogate-key-strategies.md`'s worst-case figure anyway
(`n = 2.0e5`, `dim_subscriber`'s planning-scale row count, `P ≈ 5.9e-29`) is already a wildly
conservative upper bound on the real per-entity risk, since no single subscriber in this project's
real ~50,000-subscriber, ~125,616-row dataset comes remotely close to accumulating 200,000 versions
of their own. The practical conclusion carries over unchanged from the surrogate-key document: at
md5's 128-bit width, collision probability is not a real operating concern for either identity or
change detection at any scale this project, or a comparable one, will ever reach.

## Real SQL and real column lists, from this project

`dim_subscriber`'s row_hash, `_dim_subscriber.yml`:

> md5 of (plan_tier, status) in that order via dbt_utils.generate_surrogate_key. A silver event
> whose (plan_tier, status) pair matches the current row's produces no new version.

Computed as:

```sql
{{ dbt_utils.generate_surrogate_key(['plan_tier', 'status']) }} as row_hash
```

`dim_title`'s row_hash, `_dim_title.yml`:

> md5 hash of the seven tracked columns in table order (title_name, content_type, release_year,
> runtime_minutes, maturity_rating, original_language, is_original). Drives change detection: a
> source event whose row_hash matches the currently open version's row_hash produces no new
> version.

Both descriptions carry a `not_null` test on `row_hash` in their real schema files, which, given
the null-handling mechanism above, is not a decorative test: it is the concrete regression guard
that would immediately fail the build if the coalesce-before-concatenate behavior ever broke for a
nullable tracked column like `runtime_minutes`.

Excluded from `row_hash`, `dim_subscriber` specifically (from modeling.md): `subscriber_id`
(identity), `email`, `display_name`, `country_code`, `acquisition_channel`, `current_plan_tier`,
`churn_date_key` (all Type 1, "overwrites must not spawn versions"), `previous_plan_tier` (Type 3,
"derived from the change"), `signup_date` (static, set once), all SCD tracking columns,
`is_inferred`, `loaded_at` (bookkeeping). Excluded, `dim_title`: `title_id` (identity), SCD tracking
columns, `loaded_at` (bookkeeping); `dim_title` has no Type 1 or Type 3 columns to exclude, since it
is a pure Type 2 dimension.

## When it fails, and the counter-indications

**A wrong column in the tracked list is a silent correctness bug, not a loud one.** Whether a
column is missing from `row_hash` (a real attribute change never opens a new version) or wrongly
included (an audit or Type 1 column spuriously opens one on every overwrite), the failure produces
no error at build time. The table builds, every test that only checks structural properties
(`unique`, `not_null`, interval integrity) passes, and the row count is simply wrong in a way that
looks plausible. This is why modeling.md pins the exact tracked-column list per table rather than
leaving it to be inferred from context, and why this document quotes that list in full rather than
paraphrasing it.

**Hand-computing the hash instead of calling the macro reproduces the exact drift bug
`docs/03-theory/03-surrogate-key-strategies.md` already documents for surrogate keys.**
Modeling.md's own history records this happening for real: an earlier version of the contract
described the macro's behavior as `md5(a || '||' || b...)` with `'_null_'` as the null placeholder,
which "does not match the installed dbt_utils 1.4.1 macro's real behavior (delimiter `'-'`, NULL
placeholder `'_dbt_utils_surrogate_key_null_'`)." That drift was caught on `dim_subscriber.subscriber_sk`,
a multi-component surrogate key, precisely because a single-component key never exercises the
delimiter or null-placeholder logic differently from a naive formula. `row_hash` is exactly as
exposed to this trap as any multi-component surrogate key: `dim_subscriber`'s row_hash hashes two
components, `dim_title`'s hashes seven, and any hand-rolled reconstruction of either value for a
test or a debugging query has to call the real macro, not a remembered formula, or it will silently
diverge the moment dbt_utils changes its internal delimiter or placeholder in some future version.

**The mechanism is not a defense against a hostile actor.** Like the surrogate key, `row_hash` uses
md5 for its determinism and ecosystem ubiquity, not for cryptographic collision resistance. Nothing
about this project's design assumes an adversary is trying to construct two different attribute
states that hash identically; the only threat model that matters here is accidental collision at
this project's real scale, which the math above shows is not a practical concern.

## How to verify it actually works from this repo

**The real row-count outcome is itself a large-scale, real-data proof that the mechanism
discriminates correctly, not just that it runs.** `silver_title_events` has 15,098 raw change
events across 5,000 distinct `title_id` values; of the 10,098 events that are not the initial
`catalog_add`, direct verification found that only 4,256 actually change at least one of the seven
tracked columns versus their immediate predecessor, while "the other 5,842 are exact repeats (same
title_name, content_type, release_year, runtime_minutes, maturity_rating, original_language,
is_original as the prior event) and correctly produce no new version." That 5,842-versus-4,256 split
is exactly what `row_hash` equality is deciding on every one of those 10,098 comparisons, at real
scale, against real generated data, and it is why `dim_title` builds to 9,257 rows (9,256 real
history rows plus the unknown member) rather than the ~15,000 the raw event count alone might
suggest.

**Idempotency across full rebuilds is an indirect but real check that no volatile column leaked
into the hash.** Two consecutive `dbt build --select dim_title` runs against unchanged silver input
produced, per `.notes/decisions.md`, "an identical order-independent row checksum ... byte-identical
(matching md5 of the dumped CSV)" across every column except `loaded_at`. `dim_subscriber` reports
the identical outcome across three separate full rebuilds. If `loaded_at`, or any other wall-clock
value, had leaked into `row_hash`'s input, this exact check would fail: `row_hash` itself would
differ between the two runs (since it is a persisted output column, not just an internal
comparison), and that difference would propagate into the overall row checksum immediately. This
check was not originally designed as a `row_hash`-null-handling regression test, but it structurally
catches that entire bug class as a side effect of what it does check.

**A live query anyone can run against the built warehouse**, isolating a series title specifically
to confirm the null-handling path works on real data, not just in theory:

```sql
select title_id, title_name, content_type, runtime_minutes, row_hash, scd_version
from dim_title
where content_type = 'series'
order by title_id, effective_from
limit 20;
```

Every row returned should have a non-null `row_hash` despite `runtime_minutes` being null on every
one of them; a version boundary should appear only where `title_name`, `maturity_rating`,
`original_language`, or `is_original` genuinely changed between consecutive rows for the same
`title_id`, never merely because `runtime_minutes` stayed null.

**dbt tests.** `not_null` on `row_hash` for both dimensions runs on every `dbt test --select
dim_subscriber dim_title` invocation (or `make test`); a regression in the null-coalescing path
described above would surface as an immediate test failure on the next build touching any series
title or any subscriber row with a null tracked attribute, not as a silent data-quality drift
discovered later.
