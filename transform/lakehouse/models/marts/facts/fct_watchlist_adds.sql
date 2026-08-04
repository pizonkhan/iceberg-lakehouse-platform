{{
    config(
        materialized='table'
    )
}}

-- fct_watchlist_adds (factless fact, .notes/modeling.md). Grain: one row
-- per event of one subscriber adding one title to their watchlist.
-- watchlist_event_id is the degenerate dimension and unique merge key.
-- No measures: existence of the row is the fact, analysis is done by
-- counting rows. Removals are not modeled by the source, so a re-add
-- after a removal is a legitimate new event row and (subscriber, title)
-- pairs can repeat with distinct added_at values; nothing here
-- deduplicates on that pair. ~750k rows, full-refresh table (the
-- marts-level default in dbt_project.yml already sets materialized=
-- 'table', repeated explicitly here to match every other fact model's
-- style).

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

with source as (

    select
        watchlist_event_id,
        subscriber_id,
        title_id,
        added_at
    from {{ ref('silver_watchlist_adds') }}

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
