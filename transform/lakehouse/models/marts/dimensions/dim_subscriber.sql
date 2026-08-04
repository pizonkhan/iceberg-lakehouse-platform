{{
    config(
        materialized='table'
    )
}}

-- dim_subscriber (Type 6 hybrid, .notes/modeling.md). Grain: one row per
-- subscriber per tracked-attribute version, where a version begins
-- whenever plan_tier or status changes. Full-refresh table, not
-- incremental (that conversion is a later work package): at ~200k rows
-- silver_subscriber_events is cheap to reprocess whole every build, and a
-- from-scratch SCD build is far simpler to get right than an incremental
-- merge on the first pass.
--
-- silver_subscriber_events is a change-event stream (one row per profile
-- change, not one row per subscriber), so this model does the entire
-- Type 2/3/6 versioning itself: dedup same-instant ties, detect version
-- boundaries from a (plan_tier, status) row_hash, collapse runs of
-- unchanged hash into a single version, then layer the Type 1 mirrors,
-- the Type 3 previous_plan_tier, and the churn_date_key mirror on top.
--
-- status remapping: silver carries status verbatim from source
-- (trial, active, paused, cancelled, deleted), see
-- models/intermediate/schema.yml on silver_subscriber_events, which
-- documents that gold is responsible for mapping cancelled/deleted onto
-- dim_subscriber's status domain (trial, active, paused, churned,
-- unknown). That remapping happens in the first cte below, before
-- anything else touches status, so row_hash, versioning, and
-- churn_date_key all operate on the gold vocabulary.

{% set high_date = "timestamp '9999-12-31 23:59:59.999999'" %}

with raw_events as (

    -- gold-vocabulary status remapping happens here, once, so every
    -- downstream cte (row_hash, versioning, churn detection) already
    -- sees the domain modeling.md declares (trial, active, paused,
    -- churned, unknown) instead of silver's source vocabulary.
    select
        subscriber_id,
        change_event_id,
        changed_at,
        change_type,
        email,
        display_name,
        country_code,
        acquisition_channel,
        signup_date,
        plan_tier,
        case
            when status in ('cancelled', 'deleted') then 'churned'
            else status
        end as status,
        _batch_id
    from {{ ref('silver_subscriber_events') }}

),

-- tie-break for multiple changes carrying the identical changed_at
-- timestamp for the same subscriber: order by (_batch_id, source row
-- sequence within the batch) and keep only the last, per modeling.md.
-- change_event_id is the proxy for "source row sequence within the
-- batch": it is a fixed-width, monotonically assigned id (verified
-- against the real data, its ordering matches changed_at ordering), and
-- silver_subscriber_events has no other row-sequence column. Zero exact
-- ties exist in the current 199,928-row dataset (verified), so this is a
-- defensive no-op today, same posture as the dedup step one layer down
-- in silver_subscriber_events itself.
same_instant_deduped as (

    select
        *,
        row_number() over (
            partition by subscriber_id, changed_at
            order by _batch_id desc, change_event_id desc
        ) as _tie_rank
    from raw_events

),

filtered as (

    select * from same_instant_deduped
    where _tie_rank = 1

),

hashed as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key(['plan_tier', 'status']) }} as row_hash
    from filtered

),

-- lag comparisons that drive three independent running counters:
-- version_group (increments on any row_hash change, i.e. plan_tier or
-- status), plan_segment (increments only when plan_tier itself changes),
-- status_segment (increments only when status itself changes).
-- plan_segment and status_segment are each constant across the events
-- inside one version_group by construction, since a version_group is a
-- run of events whose (plan_tier, status) pair does not change.
flagged as (

    select
        *,
        lag(row_hash) over (partition by subscriber_id order by changed_at)
            as _prev_row_hash,
        lag(plan_tier) over (partition by subscriber_id order by changed_at)
            as _prev_plan_tier,
        lag(status) over (partition by subscriber_id order by changed_at)
            as _prev_status
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
        sum(case when _prev_plan_tier is null or plan_tier <> _prev_plan_tier then 1 else 0 end)
            over (
                partition by subscriber_id order by changed_at
                rows between unbounded preceding and current row
            ) as plan_segment,
        sum(case when _prev_status is null or status <> _prev_status then 1 else 0 end)
            over (
                partition by subscriber_id order by changed_at
                rows between unbounded preceding and current row
            ) as status_segment
    from flagged

),

-- collapse each run of unchanged row_hash into a single version. min()
-- on plan_tier / status / row_hash / plan_segment / status_segment is
-- just a same-value pick: those columns are constant within a
-- version_group by construction, so min() never actually compares
-- distinct values, it only extracts the one value present.
versions as (

    select
        subscriber_id,
        version_group,
        min(changed_at) as effective_from,
        min(plan_tier) as plan_tier,
        min(status) as status,
        min(row_hash) as row_hash,
        min(plan_segment) as plan_segment,
        min(status_segment) as status_segment
    from grouped
    group by subscriber_id, version_group

),

versions_keyed as (

    select
        subscriber_id,
        version_group,
        plan_segment,
        status_segment,
        plan_tier,
        status,
        row_hash,
        effective_from,
        row_number() over (partition by subscriber_id order by effective_from) as scd_version,
        lead(effective_from) over (partition by subscriber_id order by effective_from)
            as _next_effective_from
    from versions

),

-- type 3: previous_plan_tier only ever changes at a genuine plan_tier
-- change, never at a status-only version. lag() over distinct
-- (subscriber_id, plan_segment, plan_tier) gives the plan_tier of the
-- prior plan segment; joining back on plan_segment broadcasts that same
-- value across every version row inside the current plan segment,
-- matching modeling.md's "left as written at their creation" rule for a
-- status-only change (the new row inherits the same previous_plan_tier
-- as the row before it, because it is still in the same plan_segment).
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

),

-- churn_date_key mirrors the date status first became 'churned' in the
-- subscriber's current (most recent) status segment, cleared to NULL on
-- reactivation, per modeling.md. Net effect of a full-refresh build: only
-- the FINAL status segment matters, since any transition away from
-- churned resets the mirror and a full refresh only ever materializes
-- the final state.
status_segment_summary as (

    select
        subscriber_id,
        status_segment,
        min(effective_from) as segment_start,
        min(status) as segment_status
    from versions_keyed
    group by subscriber_id, status_segment

),

latest_status_segment as (

    select
        subscriber_id,
        max(status_segment) as last_segment
    from versions_keyed
    group by subscriber_id

),

churn_lookup as (

    select
        s.subscriber_id,
        case when s.segment_status = 'churned' then s.segment_start end as churn_effective_from
    from status_segment_summary as s
    inner join latest_status_segment as l
        on s.subscriber_id = l.subscriber_id
        and s.status_segment = l.last_segment

),

-- type 1 mirrors (email, display_name, country_code, acquisition_channel,
-- current_plan_tier): overwritten across all rows of a subscriber
-- whenever silver shows a new value. In a full-refresh build the net
-- effect of "overwritten on every event, in event order" is just "the
-- chronologically last event's value", broadcast to every version row.
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

),

-- signup_date is static, set once. Every subscriber_id has exactly one
-- change_type = 'signup' event (verified: 50,000 subscribers, 50,000
-- signup events), so this is a direct lookup; the row_number guard is
-- defensive in case a future load ever produces more than one.
signup_attrs as (

    select
        subscriber_id,
        signup_date
    from (
        select
            *,
            row_number() over (partition by subscriber_id order by changed_at asc) as _rn
        from filtered
        where change_type = 'signup'
    ) as ranked
    where _rn = 1

),

regular_rows as (

    select
        {{ dbt_utils.generate_surrogate_key(['v.subscriber_id', 'v.effective_from']) }}
            as subscriber_sk,
        v.subscriber_id,
        le.email,
        le.display_name,
        le.country_code,
        le.acquisition_channel,
        sa.signup_date,
        v.plan_tier,
        v.status,
        le.current_plan_tier,
        pp.previous_plan_tier,
        cast(date_format(cl.churn_effective_from, '%Y%m%d') as integer) as churn_date_key,
        v.effective_from,
        coalesce(v._next_effective_from, {{ high_date }}) as effective_to,
        (v._next_effective_from is null) as is_current,
        v.scd_version,
        v.row_hash,
        false as is_inferred
    from versions_keyed as v
    inner join latest_event as le on v.subscriber_id = le.subscriber_id
    left join signup_attrs as sa on v.subscriber_id = sa.subscriber_id
    left join plan_prev_by_segment as pp
        on v.subscriber_id = pp.subscriber_id
        and v.plan_segment = pp.plan_segment
    left join churn_lookup as cl on v.subscriber_id = cl.subscriber_id

),

-- late-arriving subscriber self-heal (modeling.md, "Late-arriving
-- subscriber handling"). Because this dimension must be built before any
-- fact, a fact can never write into it; instead this model detects, on
-- every build, any subscriber_id referenced by a fact-side silver table
-- that does not appear anywhere in silver_subscriber_events, and
-- synthesizes the inferred row itself, so the point-in-time interval
-- join in every downstream fact always resolves.
--
-- Because this is a full-refresh build rather than an incremental merge,
-- there is no persisted prior state to "backfill in place": a
-- subscriber_id is only ever inferred here if it is completely absent
-- from silver_subscriber_events at build time. The moment real profile
-- events exist for a subscriber_id, that id no longer appears in
-- late_arriving_ids at all, so it is built entirely through the normal
-- regular_rows path above with a real effective_from at scd_version 1,
-- which is the full-refresh equivalent of modeling.md's incremental
-- "the inferred row becomes historical scd_version 1 and real changes
-- version forward from it": here there is simply never an inferred row
-- to begin with once real data exists, so the same end state is reached
-- without a separate backfill step. See .notes/decisions.md.
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

-- unknown member row, per modeling.md's general rule: sk = md5('-1'),
-- every descriptive varchar 'Unknown' (title case, distinct from the
-- inferred row's lowercase 'unknown', which is a real domain value, not
-- a sentinel), numerics/type 3/churn NULL, spanning 1900-01-01 to the
-- high date, is_current true. This is a different concept from the
-- late-arriving inferred row above: it exists so a fact FK that cannot
-- resolve any subscriber at all still has somewhere to point.
unknown_member as (

    select
        {{ dbt_utils.generate_surrogate_key(["'-1'"]) }} as subscriber_sk,
        '-1' as subscriber_id,
        'Unknown' as email,
        'Unknown' as display_name,
        'Unknown' as country_code,
        'Unknown' as acquisition_channel,
        cast(null as date) as signup_date,
        'Unknown' as plan_tier,
        'Unknown' as status,
        'Unknown' as current_plan_tier,
        cast(null as varchar) as previous_plan_tier,
        cast(null as integer) as churn_date_key,
        timestamp '1900-01-01 00:00:00.000000' as effective_from,
        {{ high_date }} as effective_to,
        true as is_current,
        1 as scd_version,
        {{ dbt_utils.generate_surrogate_key(["'Unknown'", "'Unknown'"]) }} as row_hash,
        false as is_inferred

),

unioned as (

    select * from regular_rows
    union all
    select * from inferred_rows
    union all
    select * from unknown_member

)

select
    *,
    cast(current_timestamp as timestamp(6)) as loaded_at
from unioned
