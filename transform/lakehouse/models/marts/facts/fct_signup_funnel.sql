{{
    config(
        materialized='table'
    )
}}

-- fct_signup_funnel (accumulating snapshot, .notes/modeling.md). Grain: one
-- row per subscriber signup attempt (signup_id), updated in place on every
-- full-refresh build as the attempt crosses registration, email
-- verification, payment-method-added, plan-selected, and first-stream
-- milestones, until the funnel completes or expires. This is the one fact
-- in this project built this way: every other fact is a transaction or
-- periodic-snapshot fact, one immutable row per event or per period. Here
-- a single row's milestone columns, lag measures, and funnel_status can
-- all change on a rebuild as new milestones arrive or as the 30-day
-- expiry clock passes, rather than only ever gaining new rows. ~70k rows,
-- full-refresh table (there is no incremental merge key semantics that
-- make sense here: every column of an existing row can still change).

-- subscriber_sk resolution note, read before touching this join:
-- modeling.md's point-in-time join rule (exact half-open interval,
-- registered_at >= effective_from and < effective_to) fails on this
-- fact's real data. silver_signup_funnel.registered_at is truncated to
-- whole-second precision (a source characteristic: every timestamp column
-- on this table lands exactly on the second, verified against the real
-- data), while dim_subscriber.effective_from for the same underlying
-- signup event carries the true microsecond timestamp inherited from
-- silver_subscriber_events. The true event instant is always at or after
-- the truncated registered_at, by less than one second (verified: 0 to
-- 999 milliseconds across all 62,976 registered rows in the real
-- dataset). An exact half-open join therefore misses on nearly every row
-- (verified against real data before this fix: 75 of 62,976 resolved).
-- The fix widens only the lower bound by exactly one second, the maximum
-- possible truncation error, so the true instant is always still inside
-- the widened interval; the upper bound (effective_to) needs no
-- adjustment because it is the true instant, not the truncated one, that
-- must be less than effective_to, and that already holds. Ranked to the
-- closest candidate (max effective_from) as a determinism guard in case a
-- subscriber ever has two version boundaries inside one second of each
-- other, though no such case exists in the current data (verified: 0
-- signup_id with more than one candidate). See .notes/decisions.md.

with source as (

    select
        signup_id,
        subscriber_id,
        attempt_started_at,
        registered_at,
        email_verified_at,
        payment_method_added_at,
        selected_plan_id,
        plan_selected_at,
        first_stream_at
    from {{ ref('silver_signup_funnel') }}

),

date_keyed as (

    select
        *,
        cast(date_format(attempt_started_at, '%Y%m%d') as integer) as signup_date_key,
        case when registered_at is not null
            then cast(date_format(registered_at, '%Y%m%d') as integer)
            end as registered_date_key,
        case when email_verified_at is not null
            then cast(date_format(email_verified_at, '%Y%m%d') as integer)
            end as email_verified_date_key,
        case when payment_method_added_at is not null
            then cast(date_format(payment_method_added_at, '%Y%m%d') as integer)
            end as payment_method_added_date_key,
        case when plan_selected_at is not null
            then cast(date_format(plan_selected_at, '%Y%m%d') as integer)
            end as plan_selected_date_key,
        case when first_stream_at is not null
            then cast(date_format(first_stream_at, '%Y%m%d') as integer)
            end as first_stream_date_key
    from source

),

-- candidate dim_subscriber versions current at registered_at, with the
-- one-second lower-bound widening explained above. Ranked so that if more
-- than one candidate ever qualifies, the closest one (largest
-- effective_from, i.e. the version that started nearest to the truncated
-- registered_at) wins.
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

subscriber_resolved as (

    select signup_id, subscriber_sk
    from subscriber_candidates
    where _rn = 1

),

-- dim_plan carries no version history (Type 3, one row per plan_id), so
-- this is a direct natural-key join, no point-in-time predicate needed.
plan_resolved as (

    select distinct
        d.signup_id,
        dp.plan_sk
    from date_keyed as d
    inner join {{ ref('dim_plan') }} as dp
        on dp.plan_id = d.selected_plan_id
    where d.plan_selected_at is not null

),

-- coalesce is a guard only, matching the pattern in modeling.md's
-- point-in-time join rule: subscriber_resolved and plan_resolved are
-- expected to cover every row that has reached the corresponding
-- milestone (verified against real data), so a NULL surviving to here
-- means an unexpected resolution gap, not the normal pre-milestone state
-- (that state is handled by the left join itself finding no row on the
-- unregistered / no-plan-selected side).
keyed as (

    select
        d.*,
        coalesce(sr.subscriber_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as subscriber_sk,
        coalesce(pr.plan_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as plan_sk
    from date_keyed as d
    left join subscriber_resolved as sr on d.signup_id = sr.signup_id
    left join plan_resolved as pr on d.signup_id = pr.signup_id

),

-- lag measures at second-then-hour precision (source timestamps are
-- whole-second, see the note above; dividing seconds by 3600.0 rather
-- than using date_diff('hour', ...) directly preserves fractional hours
-- instead of truncating to whole hours). Each is NULL until both of its
-- prerequisite milestones are present, per modeling.md's explicit
-- nullability rule for this table, enforced by a data test.
lagged as (

    select
        *,
        case when registered_at is not null and email_verified_at is not null
            then cast(date_diff('second', registered_at, email_verified_at) / 3600.0 as decimal(10, 2))
            end as hours_registration_to_verification,
        case when email_verified_at is not null and payment_method_added_at is not null
            then cast(date_diff('second', email_verified_at, payment_method_added_at) / 3600.0 as decimal(10, 2))
            end as hours_verification_to_payment,
        case when registered_at is not null and first_stream_at is not null
            then cast(date_diff('second', registered_at, first_stream_at) / 3600.0 as decimal(10, 2))
            end as hours_registration_to_first_stream
    from keyed

),

-- single build-time instant, reused for both funnel_status's expiry
-- check and loaded_at, so the two are always consistent within one run.
-- funnel_status is deliberately evaluated against wall-clock time, not
-- any source column: per modeling.md, an in_progress attempt whose
-- registered_at crosses the 30-day expiry threshold becomes expired on
-- the next build with no change to the underlying silver row at all.
-- That time-dependence is intentional for an accumulating snapshot, not
-- a bug: the answer to "is this attempt still viable" genuinely changes
-- as time passes, so funnel_status (and is_completed, which is derived
-- from it) can legitimately differ between two builds run on different
-- days even against byte-identical source data.
build_time as (

    select cast(current_timestamp as timestamp(6)) as now_ts

),

status_computed as (

    select
        l.*,
        case
            when l.first_stream_at is not null then 'completed'
            when l.registered_at is not null
                and bt.now_ts >= l.registered_at + interval '30' day then 'expired'
            else 'in_progress'
        end as funnel_status,
        bt.now_ts
    from lagged as l
    cross join build_time as bt

)

select
    signup_id,
    subscriber_sk,
    plan_sk,
    signup_date_key,
    registered_date_key,
    email_verified_date_key,
    payment_method_added_date_key,
    plan_selected_date_key,
    first_stream_date_key,
    registered_at,
    email_verified_at,
    payment_method_added_at,
    plan_selected_at,
    first_stream_at,
    hours_registration_to_verification,
    hours_verification_to_payment,
    hours_registration_to_first_stream,
    funnel_status,
    (funnel_status = 'completed') as is_completed,
    now_ts as loaded_at
from status_computed
