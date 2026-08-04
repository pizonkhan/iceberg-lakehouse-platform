{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='watchlist_event_id',
        on_schema_change='fail'
    )
}}

-- fct_watchlist_adds (factless fact, .notes/modeling.md). Grain: one row
-- per event of one subscriber adding one title to their watchlist.
-- watchlist_event_id is the degenerate dimension and unique merge key.
-- No measures: existence of the row is the fact, analysis is done by
-- counting rows. Removals are not modeled by the source, so a re-add
-- after a removal is a legitimate new event row and (subscriber, title)
-- pairs can repeat with distinct added_at values; nothing here
-- deduplicates on that pair. ~750k rows.

-- Incremental via Iceberg MERGE (incremental_strategy='merge'), converted
-- from the original full-refresh table. The watermark that decides which
-- silver rows are "new to this run" is bronze's _ingested_at, carried
-- unchanged through silver_watchlist_adds, NOT added_at (the business
-- event time). silver_watchlist_adds itself is still rebuilt full-refresh
-- every run, but it passes through each bronze row's original
-- _ingested_at untouched (see that model), so filtering on
-- "_ingested_at > the highest _ingested_at already loaded into {{ this }}"
-- correctly identifies rows genuinely new to bronze on this run
-- regardless of how old their added_at happens to be. A watermark on
-- added_at would silently drop any late-arriving watchlist event whose
-- add date is old relative to when it was actually ingested, since such a
-- row would never be "new" by an added_at test once the added_at range it
-- falls in has already been scanned once. See .notes/decisions.md for the
-- concrete check of whether this table's real data currently exercises
-- that case.

-- {{ this }} itself does not carry _ingested_at: the gold column contract
-- (modeling.md) is fixed at six columns and bronze/silver metadata is
-- deliberately not one of them, so the watermark cannot be read directly
-- off the fact table with "select max(_ingested_at) from {{ this }}"
-- (tried first; Trino rejects it as an unsupported correlated subquery,
-- because _ingested_at does not exist on {{ this }} and the planner
-- silently binds the name to the outer query's silver alias instead of
-- failing with a column-not-found error, confirmed directly in Trino
-- before writing the fix below). Instead the watermark CTE below
-- reconstructs each already-loaded row's original _ingested_at by joining
-- {{ this }} back to silver_watchlist_adds on the shared grain key
-- (watchlist_event_id) and taking the max over that join; this stays
-- correct because silver's _ingested_at for a given watchlist_event_id
-- never changes across silver's own full rebuilds (it is bronze metadata
-- passed through verbatim, not recomputed). coalesce() defaults the very
-- first build (when {{ this }} has no rows yet, so the join and its max
-- are empty) to a sentinel far before any real bronze ingestion so
-- nothing is spuriously excluded on that build; in practice
-- is_incremental() is false on that build anyway (no existing relation),
-- so the coalesce only matters if a later run ever finds an
-- existing-but-empty table.

-- Backfill: to force a reprocess of a specific _ingested_at range through
-- the same MERGE path (matched watchlist_event_ids are updated in place,
-- unmatched ones inserted; never a drop or full-refresh), pass both
-- backfill_start and backfill_end vars, half-open [start, end), for
-- example:
--   dbt build --select fct_watchlist_adds --target trino \
--     --vars '{"backfill_start": "2026-08-04 00:00:00.000000", "backfill_end": "2026-08-05 00:00:00.000000"}'
-- Both vars must be given together; either one alone is ignored and the
-- normal watermark filter applies. The backfill filter only takes effect
-- when is_incremental() is true (an existing incremental table, no
-- --full-refresh flag): on the very first build, or any --full-refresh,
-- every silver row is loaded regardless of these vars, since there is
-- nothing to "backfill into" yet and a partial first load would be a
-- correctness bug, not a feature.

-- subscriber_sk and title_sk resolution: the literal point-in-time
-- interval-overlap predicate modeling.md's "Point-in-time join rule"
-- gives for this fact (added_at against each SCD dimension's half-open
-- [effective_from, effective_to)), widened on the lower bound by exactly
-- one second per the "timestamp precision mismatch" mitigation paragraph
-- in that same section. silver_watchlist_adds.added_at is whole-second
-- only (verified against the real data: 0 of 750,000 rows carry a
-- sub-second component), while dim_subscriber.effective_from and
-- dim_title.effective_from carry true microsecond precision, so an exact
-- half-open join can miss by up to 999 milliseconds of truncation error
-- exactly as it did for fct_signup_funnel. Verified before writing this
-- join: the exact predicate (no widening) misses subscriber_sk on 82 of
-- 750,000 rows and title_sk on 0 of 750,000 rows; the widened predicate
-- misses both on 0 of 750,000 rows, with no new ambiguity introduced (the
-- minimum real gap between two consecutive versions of the same natural
-- key is 221,062 ms for dim_subscriber and 357,538,399 ms for dim_title,
-- both far over the 1 second widening, and a direct check of the widened
-- join found at most 1 candidate row per watchlist_event_id for both
-- dimensions, confirmed by count, not assumed). Ranked to the closest
-- candidate (max effective_from) as the same determinism guard
-- fct_signup_funnel uses, in case a future data refresh ever produces a
-- sub-one-second version gap.

-- title_sk in particular was the point of this work package's explicit
-- verification instruction: dim_title used to have no self-heal path and
-- a prior generator defect (fixed in commit b1b4401, see
-- .notes/failures.md) let 4,833 of 5,000 titles have playback sessions
-- timestamped before their own first catalog version, driving
-- fct_playback_events' title_sk to the unknown member on 17.3% of rows
-- before the fix. Confirmed directly against this fact's own source
-- (silver_watchlist_adds is a different file than silver_playback_
-- sessions, so this is a real, independent check, not an assumption
-- carried over from playback's verification): title_sk resolves to the
-- unknown member on 0 of 750,000 rows here, both with and without the
-- one-second widening. The bug did not resurface in this fact.

with with_metadata as (

    select
        watchlist_event_id,
        subscriber_id,
        title_id,
        added_at,
        _ingested_at
    from {{ ref('silver_watchlist_adds') }}

),

{% if is_incremental() and (var('backfill_start', none) is none or var('backfill_end', none) is none) %}
-- normal incremental run only: reconstruct the watermark by joining the
-- rows already loaded into {{ this }} back to silver on the shared grain
-- key to recover their original _ingested_at (see top-of-file description
-- for why {{ this }} cannot be queried for _ingested_at directly).
watermark as (

    select coalesce(max(s._ingested_at), timestamp '1900-01-01 00:00:00.000000') as watermark_at
    from {{ this }} as f
    inner join {{ ref('silver_watchlist_adds') }} as s
        on f.watchlist_event_id = s.watchlist_event_id

),
{% endif %}

source as (

    select
        watchlist_event_id,
        subscriber_id,
        title_id,
        added_at
    from with_metadata

    {% if is_incremental() %}
        {% if var('backfill_start', none) is not none and var('backfill_end', none) is not none %}
    -- backfill mode: explicit _ingested_at range overrides the normal
    -- watermark, reprocessed through the same MERGE (see top-of-file
    -- description for invocation).
    where _ingested_at >= timestamp '{{ var("backfill_start") }}'
      and _ingested_at <  timestamp '{{ var("backfill_end") }}'
        {% else %}
    -- normal incremental run: only rows genuinely new to bronze since the
    -- last run, by _ingested_at, never by added_at (see top-of-file
    -- description for why).
    where _ingested_at > (select watermark_at from watermark)
        {% endif %}
    {% endif %}

),

date_keyed as (

    select
        *,
        cast(date_format(added_at, '%Y%m%d') as integer) as added_date_key
    from source

),

-- candidate dim_subscriber versions current at added_at, lower bound
-- widened by one second for the whole-second truncation mismatch
-- described above. Ranked so that if more than one candidate ever
-- qualifies, the closest one (largest effective_from) wins; verified
-- against real data that no watchlist_event_id currently has more than
-- one candidate.
subscriber_candidates as (

    select
        d.watchlist_event_id,
        ds.subscriber_sk,
        row_number() over (
            partition by d.watchlist_event_id order by ds.effective_from desc
        ) as _rn
    from date_keyed as d
    inner join {{ ref('dim_subscriber') }} as ds
        on ds.subscriber_id = d.subscriber_id
        and d.added_at + interval '1' second >= ds.effective_from
        and d.added_at < ds.effective_to

),

subscriber_resolved as (

    select watchlist_event_id, subscriber_sk
    from subscriber_candidates
    where _rn = 1

),

-- identical pattern against dim_title.
title_candidates as (

    select
        d.watchlist_event_id,
        dt.title_sk,
        row_number() over (
            partition by d.watchlist_event_id order by dt.effective_from desc
        ) as _rn
    from date_keyed as d
    inner join {{ ref('dim_title') }} as dt
        on dt.title_id = d.title_id
        and d.added_at + interval '1' second >= dt.effective_from
        and d.added_at < dt.effective_to

),

title_resolved as (

    select watchlist_event_id, title_sk
    from title_candidates
    where _rn = 1

)

select
    d.watchlist_event_id,
    coalesce(sr.subscriber_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as subscriber_sk,
    coalesce(tr.title_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as title_sk,
    d.added_date_key,
    d.added_at,
    cast({{ dbt.current_timestamp() }} as timestamp(6)) as loaded_at
from date_keyed as d
left join subscriber_resolved as sr on d.watchlist_event_id = sr.watchlist_event_id
left join title_resolved as tr on d.watchlist_event_id = tr.watchlist_event_id
