{{
    config(
        materialized='table'
    )
}}

-- fct_playback_events (.notes/modeling.md). Grain: one row per completed
-- playback session (playback_session_id is the degenerate dimension and
-- the unique key, no separate surrogate key on this fact). ~119.6M rows,
-- the dominant volume table in the project. Full-refresh table this work
-- package, not incremental (that conversion is a separate later work
-- package).
--
-- subscriber_sk and title_sk resolve through the point-in-time interval
-- join against each SCD dimension's [effective_from, effective_to) on
-- session_started_at, exactly as modeling.md's "Point-in-time join rule"
-- section literally specifies for this fact: equality on the natural key
-- plus a half-open range predicate on the event timestamp. Because both
-- dimensions' version intervals are contiguous and non-overlapping
-- (enforced by their own SCD tests), each join matches at most one
-- version. A left join is used, not inner, because a match can
-- legitimately fail (see below), and the miss is repaired with
-- coalesce() to the unknown member's key rather than dropping the row.
--
-- device_sk is a plain equality FK, not versioned: dim_device is Type 1,
-- current state only, so there is no interval to resolve against, just
-- device_id = device_id, coalesced to unknown when device_id is null
-- (the null-device pathology, ~2% of rows) or the device_id has no
-- match.
--
-- session_date_key is computed directly from session_started_at with the
-- same cast(date_format(..., '%Y%m%d') as integer) formula dim_date uses
-- for its own date_key, rather than joining to dim_date: date_key is a
-- pure function of the calendar date, dim_date's own grain covers every
-- date this fact can produce (2023-01-01 through 2027-12-31, verified
-- against real session_started_at bounds when dim_date was built), and
-- computing it directly avoids a fourth join over a ~120M-row table on
-- infrastructure that is already tight on a single full-width scan of
-- this table alone. dim_subscriber's churn_date_key mirror uses the
-- identical formula for the same reason. See .notes/decisions.md.
--
-- Memory note: this project's single-node Trino caps query memory at
-- 1.5GB per node (infra/trino/etc/config.properties), and a bare
-- full-width scan of the playback table alone sits right at that ceiling
-- (see .notes/decisions.md, silver layer notes). The base CTE below
-- therefore projects only the 9 columns this fact actually needs, the
-- same narrow-projection technique that made the silver quality gate's
-- reject-side queries reliable at this table's scale, rather than
-- selecting silver_playback_sessions' full 15-column width and letting
-- three broadcast joins ride on top of that. The trino target's
-- session_properties in profiles.yml (task_concurrency, writer
-- concurrency) already apply to every query against this target,
-- including this one; no additional per-model session property override
-- was needed once the projection was narrowed, confirmed by the real
-- build completing under those settings without a model-level override.

with playback as (

    select
        playback_session_id,
        subscriber_id,
        title_id,
        device_id,
        session_started_at,
        session_ended_at,
        cast(watch_duration_seconds as integer) as watch_duration_seconds,
        cast(buffering_count as integer) as buffering_events,
        cast(avg_bitrate_kbps as integer) as avg_bitrate_kbps
    from {{ ref('silver_playback_sessions') }}

),

-- point-in-time subscriber resolution. dim_subscriber's own late-arriving
-- self-heal guarantees every real subscriber_id resolves to something
-- other than the unknown member; a small residual miss is still possible
-- here (see .notes/decisions.md for the real count and root cause found
-- while building this model: a handful of subscribers have playback rows
-- timestamped at or a few dozen microseconds before their own first
-- dim_subscriber version's effective_from), which is exactly the kind of
-- case the coalesce below exists to catch rather than fail the load on.
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

-- point-in-time title resolution, identical predicate shape against
-- dim_title. Unlike dim_subscriber, dim_title has no late-arriving
-- self-heal (titles are meant to arrive from a controlled catalog feed
-- ahead of playback per modeling.md), so a miss here legitimately takes
-- the unknown member. runtime_minutes is carried through unresolved for
-- completion_pct: null on any row where this join does not match, which
-- correctly covers both "no version of this title covers this instant"
-- and "matched a real version, but that version's runtime_minutes is
-- itself null" (every series row) in one condition.
title_resolved as (

    select
        s.*,
        dt.title_sk,
        dt.runtime_minutes
    from subscriber_resolved as s
    left join {{ ref('dim_title') }} as dt
        on dt.title_id = s.title_id
        and s.session_started_at >= dt.effective_from
        and s.session_started_at < dt.effective_to

),

-- device resolution: plain equality, no interval, dim_device is Type 1.
device_resolved as (

    select
        t.*,
        dd.device_sk
    from title_resolved as t
    left join {{ ref('dim_device') }} as dd
        on dd.device_id = t.device_id

),

-- completion_pct computed once here, then reused by both completion_pct
-- itself and is_completed in the final select, rather than repeating the
-- case expression twice.
measures as (

    select
        d.*,
        cast(
            case
                when d.runtime_minutes is null then null
                else least(
                    cast(d.watch_duration_seconds as double) / (d.runtime_minutes * 60.0),
                    1.0
                )
            end as decimal(5, 4)
        ) as completion_pct
    from device_resolved as d

),

final as (

    select
        playback_session_id,
        coalesce(subscriber_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as subscriber_sk,
        coalesce(title_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as title_sk,
        coalesce(device_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as device_sk,
        cast(date_format(session_started_at, '%Y%m%d') as integer) as session_date_key,
        session_started_at,
        session_ended_at,
        watch_duration_seconds,
        completion_pct,
        buffering_events,
        avg_bitrate_kbps,
        coalesce(completion_pct >= 0.9000, false) as is_completed,
        cast({{ dbt.current_timestamp() }} as timestamp(6)) as loaded_at
    from measures

)

select * from final
