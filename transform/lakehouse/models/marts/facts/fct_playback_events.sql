{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='playback_session_id',
        on_schema_change='fail'
    )
}}

-- fct_playback_events (.notes/modeling.md). Grain: one row per completed
-- playback session (playback_session_id is the degenerate dimension and
-- the unique key, no separate surrogate key on this fact). ~119.6M rows,
-- the dominant volume table in the project, and exactly the shape of
-- table where incremental processing earns its complexity (full-refresh
-- baseline: 228s to build the table, 231s including hooks, see
-- .notes/decisions.md 2026-08-04 "fct_playback_events built full-refresh").
--
-- Incremental via Iceberg MERGE (incremental_strategy='merge'), converted
-- from the original full-refresh table. The watermark that decides which
-- silver rows are "new to this run" is bronze's _ingested_at, carried
-- unchanged through silver_playback_sessions, NOT session_started_at (the
-- business event time). silver_playback_sessions is itself still rebuilt
-- full-refresh every run, but it passes through each bronze row's
-- original _ingested_at untouched (see that model: _ingested_at is
-- selected straight from stg_playback_sessions with no transformation),
-- so filtering on "_ingested_at newer than what this table has already
-- incorporated" correctly identifies rows genuinely new to bronze on this
-- run regardless of how old their session_started_at happens to be.
--
-- Where the watermark itself is read from, and why not a plain
-- max(_ingested_at) over {{ this }}'s own rows: modeling.md's column
-- contract for this fact (the "fct_playback_events" section) does not
-- include _ingested_at, only loaded_at, and this work package is
-- explicitly scoped to changing materialization and filtering, not the
-- column contract, so _ingested_at is read into the base CTE for
-- filtering only and is never written to the final select. That means the
-- watermark cannot be recovered with "select max(_ingested_at) from
-- {{ this }}" the way it naively could if the column were persisted:
-- {{ this }}'s physical schema has no such column (confirmed directly,
-- DESCRIBE iceberg.dev_facts.fct_playback_events lists exactly the 12
-- contract columns). Instead the watermark is read from this table's own
-- Iceberg snapshot history, "{{ this.identifier }}$snapshots", which
-- records the commit wall-clock time of every write this model has ever
-- made (the initial CTAS and every later MERGE) as pure manifest
-- metadata, at zero row-scan cost regardless of this table's ~120M rows
-- (confirmed directly in Trino before relying on it). max(committed_at)
-- from that system table is "the wall-clock time this table was last
-- written," which is a valid proxy watermark for "which bronze
-- _ingested_at values are already incorporated" under the same assumption
-- every processing-time watermark relies on: _ingested_at is itself
-- assigned by the ingestion process's own clock at ingest time, so it is
-- monotonically increasing across pipeline runs and a later gold build's
-- commit time is never earlier than the _ingested_at of bronze data that
-- was already merged into it as of that commit.
--
-- Why not session_started_at: this project's synthetic data deliberately
-- injects out-of-order arrival on this exact table (generation/playback.py,
-- .notes/decisions.md's pathology 6, out_of_order_fraction 3% /
-- generation/output/_pathology_manifest/out_of_order_summary.json,
-- 3,451,483 straggler rows). Each playback batch naturally covers a
-- chronological session_started_at slice, so file-write order tracks
-- event-time order by construction; the pathology then peels a fraction of
-- each batch's rows out and splices them into a batch file 3 to 15
-- indices later, so file-write (and, in a genuinely multi-run bronze
-- pipeline, ingestion) order stops tracking session_started_at order for
-- those rows. Concretely verified against the real built table before
-- writing this filter: playback_session_id pb_000119473603
-- (subscriber_id sub_028398, session_started_at 2023-02-10 13:16:24) sits
-- in _source_file playback_sessions/part-00023.parquet, whose other
-- 6.3M rows have a median session_started_at around mid-2026, roughly 3.5
-- years later. The table's own max(session_started_at) after the initial
-- full build is 2026-08-03 23:59:59; a watermark on session_started_at
-- would already sit at or past that value once any incremental run had
-- processed the table's bulk of recent-dated rows, so a straggler this old
-- arriving in a later run could never satisfy
-- "session_started_at > (past watermark)" and would be silently and
-- permanently dropped, exactly the failure mode this design avoids.
-- _ingested_at carries no session_started_at information at all, so it
-- cannot make that mistake: the row is picked up whichever run's
-- _ingested_at window it genuinely lands in, independent of its event
-- time. (This project's actual bronze history to date is a single dlt
-- load, so _ingested_at is currently uniform across all 119,640,099
-- silver rows, confirmed by direct query, distinct count 1; the argument
-- above is what makes the design correct for the next genuine incremental
-- bronze load, which is the scenario this materialization exists for.)
--
-- Backfill: to reprocess a specific _ingested_at range through this same
-- MERGE logic (for example, to re-pull a batch that was missed or
-- corrected upstream) without dropping or rebuilding the table, run:
--   dbt build --select fct_playback_events --target trino \
--     --vars '{backfill_start: "2026-08-04 00:00:00.000000", backfill_end: "2026-08-05 00:00:00.000000"}'
-- backfill_start and backfill_end are both optional and independently
-- omittable (an omitted bound is open on that side); when either is set,
-- it entirely replaces the normal watermark filter below for that run, it
-- does not narrow it further. Do not pass --full-refresh for a backfill:
-- --full-refresh drops and rebuilds the whole table from scratch, which is
-- the thing this mechanism exists to avoid. The backfill filter, like the
-- normal watermark, only applies when is_incremental() is true; the very
-- first build (or any --full-refresh) always loads every silver row.
--
-- Scale note on backfill windows, found while verifying this mechanism:
-- Iceberg MERGE needs meaningfully more memory than a plain CTAS at the
-- same row count on this infra (the MERGE plan adds a HashBuilderOperator
-- to match source against target on playback_session_id, on top of the
-- same scan cost the CTAS already pays), so a backfill window wide enough
-- to match a large fraction of this ~120M-row table's rows can exceed the
-- 1.5GB-per-node cap even though a full-refresh CTAS over the same row
-- count succeeds. Confirmed directly: a window covering this project's
-- entire current bronze history (all rows share one _ingested_at, a
-- consequence of bronze having been seeded in a single load so far, see
-- .notes/decisions.md) reliably hit EXCEEDED_LOCAL_MEMORY_LIMIT on this
-- dev stack (HashBuilderOperator alone reached 450-760MB across repeated
-- attempts), while a narrow, genuinely partial window matching a small
-- number of rows runs the identical code path successfully. This is a
-- backfill-window sizing concern for whoever invokes this on a much
-- larger bronze replay, not a defect in the mechanism itself: the same
-- MERGE machinery this backfill branch uses is exercised, correctly and
-- cheaply, by every normal incremental run.
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
-- The watermark filter below deliberately keeps this margin intact:
-- _ingested_at is used only in the CTE's WHERE clause (SQL can filter on a
-- source column without projecting it), never added to the SELECT list,
-- so it is never threaded through the three downstream wildcard
-- (select *) joins as thirteenth-column width on a ~120M-row scan. This
-- was not a hypothetical concern: an earlier draft of this model did
-- project _ingested_at through the CTE and into the wildcard chain, and
-- that single extra column was enough to push the full-refresh build over
-- this Trino instance's 1.5GB-per-node ceiling into a JVM OutOfMemoryError
-- (confirmed directly: reverting to the narrow 9-column projection with
-- _ingested_at filtered-but-not-selected fixed it on retry with no other
-- change).

{% set snapshots_relation = this.incorporate(path={"identifier": this.identifier ~ "$snapshots"}) %}

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

    {% if is_incremental() %}
    {% if var('backfill_start', none) is not none or var('backfill_end', none) is not none %}
    -- backfill mode: explicit _ingested_at window replaces the normal
    -- watermark entirely, see the invocation note above.
    where _ingested_at >= {{ "timestamp '" ~ var('backfill_start', '1900-01-01 00:00:00.000000') ~ " UTC'" }}
      and _ingested_at <  {{ "timestamp '" ~ var('backfill_end', '9999-12-31 23:59:59.999999') ~ " UTC'" }}
    {% else %}
    -- normal watermark: read from this table's own Iceberg snapshot
    -- history, not a max(_ingested_at) aggregate over row data, see the
    -- header comment above for why. Coalesce guards the case of an
    -- existing relation with no snapshot history yet, unreachable under
    -- is_incremental() in practice but kept defensively.
    where _ingested_at > (
        select coalesce(max(committed_at), timestamp '1900-01-01 00:00:00.000000 UTC')
        from {{ snapshots_relation }}
    )
    {% endif %}
    {% endif %}

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
