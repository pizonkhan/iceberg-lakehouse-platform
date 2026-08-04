{{
    config(
        materialized='table'
    )
}}

-- fct_daily_subscription_snapshot (periodic snapshot, .notes/modeling.md).
-- Grain: one row per subscriber per calendar day, capturing plan, status,
-- and MRR contribution as of end of day. Unique key: (snapshot_date_key,
-- subscriber_sk).
--
-- Row generation strategy: NOT a cross join of the ~50k subscriber roster
-- against the full ~1,826-row dim_date spine followed by a filter (that
-- shape materializes a ~91M-row intermediate before dropping it back down
-- to the real ~27M rows the window actually needs, and this single-node
-- Trino's 1.5GB per-query memory cap already struggles with wide scans at
-- tens of millions of rows, per .notes/open-questions.md's playback entry
-- and the tuning note in profiles.yml). Instead, each subscriber's exact
-- day range is generated directly with `sequence(signup_date, end_date)`
-- exploded via `cross join unnest`, so the row-generation step produces
-- exactly the rows the grain requires and nothing more.
--
-- Snapshot horizon: bounded to the latest day this pipeline actually has
-- real data for, not the full dim_date range out to 2027-12-31 (dim_date
-- is a fixed calendar spine built well ahead of the data, see dim_date.sql).
-- Computed as greatest(max(dim_subscriber.effective_from)::date,
-- max(silver_billing_ledger.transaction_posted_at)::date): those are the
-- two sources this model actually reads that carry a real event clock
-- (dim_subscriber's effective_from is itself sourced from
-- silver_subscriber_events' changed_at, including every subscriber's
-- initial version, so it already reflects the newest signup or profile
-- change observed). Verified directly against real data at build time:
-- both sources cap out at 2026-08-03, so horizon_date = 2026-08-03, well
-- short of dim_date's 2027-12-31 upper bound. See .notes/decisions.md.
--
-- The end-of-day snapshot instant is built as a direct string-cast
-- literal (`snapshot_date || ' 23:59:59.999999'`) rather than
-- `cast(snapshot_date as timestamp) + interval '1' day - interval '1'
-- microsecond`, modeling.md's literal formula: Trino's INTERVAL DAY TO
-- SECOND arithmetic only carries millisecond precision (confirmed
-- directly: `interval '0.000001' second` silently rounds to zero, and
-- `date_add('millisecond', -1, ...)` is the finest unit date_add accepts,
-- giving 23:59:59.999000, not .999999). The string-cast literal reaches
-- the identical instant modeling.md specifies (the last microsecond of
-- the day) at true timestamp(6) precision without relying on interval
-- arithmetic Trino cannot represent at that precision. See
-- .notes/decisions.md.
--
-- subscriber_sk resolves through the same point-in-time interval-overlap
-- predicate every other fact uses against dim_subscriber's half-open
-- [effective_from, effective_to), evaluated at the snapshot instant
-- rather than an event timestamp, per modeling.md's point-in-time join
-- rule. Because the day-range roster is generated directly from
-- dim_subscriber's own current rows (signup_date always falls inside that
-- subscriber's scd_version 1 interval, verified: 0 mismatches between
-- signup_date and the calendar date of each subscriber's earliest
-- effective_from across all 50,000 real subscribers), this join resolves
-- for every generated row in the real data; the coalesce to the unknown
-- member is a guard only, matching the posture modeling.md describes for
-- every other fact's point-in-time join.
--
-- plan_sk resolution follows the rule modeling.md's plan_sk clarification
-- paragraph gives, not a join against dim_subscriber's plan_tier:
-- dim_subscriber tracks plan_tier (many plan_id rows share one tier, 7 to
-- 8 plan rows per tier confirmed against dim_plan), so the only source of
-- an actual plan_id tied to a subscriber is silver_billing_ledger. This
-- model builds a plan-history interval table directly from
-- silver_billing_ledger (mirroring the same half-open interval shape
-- dim_subscriber itself uses, but keyed by billing event rather than
-- profile event) and resolves plan_sk as the plan_id from the most recent
-- transaction with transaction_posted_at at or before the snapshot
-- instant, then joins that plan_id to dim_plan. Before any billing
-- transaction has occurred for a subscriber, no interval matches and
-- plan_sk falls back to dim_plan's unknown member (verified: 1,394,403 of
-- the real fact's rows fall before that subscriber's first billing
-- transaction, mostly unbilled trial days, average 27.9 days per
-- subscriber with any pre-billing gap at all).
--
-- Billing tie-break: 19 (subscriber_id, transaction_posted_at) pairs in
-- the real data carry more than one transaction at the identical instant
-- (one pathological case, sub_048887, has 105 transactions across 29
-- distinct plan_id values at one timestamp). Deduplicated the same way
-- silver_billing_ledger dedups its own retry duplicates: partition by
-- (subscriber_id, transaction_posted_at), order by _ingested_at desc,
-- billing_transaction_id desc (as the final deterministic tie-break,
-- since _ingested_at alone does not guarantee a total order here), keep
-- rank 1. See .notes/decisions.md.

{% set high_date = "timestamp '9999-12-31 23:59:59.999999'" %}

with horizon as (

    select
        greatest(
            (select cast(max(effective_from) as date) from {{ ref('dim_subscriber') }} where subscriber_id <> '-1'),
            (select cast(max(transaction_posted_at) as date) from {{ ref('silver_billing_ledger') }})
        ) as horizon_date

),

churn_dates as (

    -- resolves churn_date_key (INTEGER, YYYYMMDD) back to a DATE via
    -- dim_date rather than hand-parsing the integer, single source of
    -- truth for the date_key <-> date_day mapping.
    select date_key, date_day
    from {{ ref('dim_date') }}

),

roster as (

    -- the subscriber roster this fact spans: current dim_subscriber rows
    -- with a known signup_date. Filtering on signup_date is not null
    -- excludes both the unknown member row and any late-arriving inferred
    -- row in the same step: neither has an observed signup date, so
    -- neither has a day range this fact can construct (0 inferred rows
    -- exist in the real data today, but the filter holds regardless).
    select
        ds.subscriber_id,
        ds.signup_date,
        ds.status as current_status,
        cd.date_day as churn_date
    from {{ ref('dim_subscriber') }} as ds
    left join churn_dates as cd on cd.date_key = ds.churn_date_key
    where ds.is_current
      and ds.signup_date is not null

),

roster_bounded as (

    -- row existence window per modeling.md: signup_date through the
    -- earlier of churn_date and horizon_date. churned subscribers stop
    -- accruing rows the day after churn, i.e. the last row is the churn
    -- day itself, so end_date = churn_date exactly, not churn_date - 1.
    -- The outer least(...) against horizon_date is a defensive clamp
    -- (every churn_date_key in the real data already falls at or before
    -- horizon_date), not something the real data currently exercises.
    select
        r.subscriber_id,
        r.signup_date,
        least(
            case
                when r.current_status = 'churned' and r.churn_date is not null then r.churn_date
                else h.horizon_date
            end,
            h.horizon_date
        ) as end_date
    from roster as r
    cross join horizon as h

),

subscriber_days as (

    -- direct per-subscriber day-range generation, see the model
    -- description above for why this replaces a full cross join against
    -- dim_date. The end_date >= signup_date guard is defensive: every
    -- subscriber's end_date is derived from a churn_date or horizon_date
    -- that is always at or after signup_date in the real data, but
    -- sequence() errors on a descending range with no explicit negative
    -- step, so this filter keeps a future data anomaly from failing the
    -- whole build instead of just dropping that one subscriber.
    select
        rb.subscriber_id,
        rb.signup_date,
        d as snapshot_date
    from roster_bounded as rb
    cross join unnest(sequence(rb.signup_date, rb.end_date)) as t (d)
    where rb.end_date >= rb.signup_date

),

snapshot_grid as (

    select
        subscriber_id,
        snapshot_date,
        cast(date_format(snapshot_date, '%Y%m%d') as integer) as snapshot_date_key,
        cast(date_diff('day', signup_date, snapshot_date) + 1 as integer) as tenure_days,
        -- see the model description: string-cast literal, not interval
        -- arithmetic, to reach true microsecond precision.
        cast(cast(snapshot_date as varchar) || ' 23:59:59.999999' as timestamp(6)) as snapshot_instant
    from subscriber_days

),

billing_deduped as (

    select
        subscriber_id,
        plan_id,
        transaction_posted_at,
        row_number() over (
            partition by subscriber_id, transaction_posted_at
            order by _ingested_at desc, billing_transaction_id desc
        ) as _tie_rank
    from {{ ref('silver_billing_ledger') }}

),

billing_events as (

    select subscriber_id, plan_id, transaction_posted_at
    from billing_deduped
    where _tie_rank = 1

),

billing_intervals as (

    -- plan-history interval per subscriber, built directly from billing
    -- events rather than a physical dimension: half-open
    -- [plan_valid_from, plan_valid_to), same interval shape as
    -- dim_subscriber's own SCD mechanics, one row per billing transaction.
    select
        subscriber_id,
        plan_id,
        transaction_posted_at as plan_valid_from,
        coalesce(
            lead(transaction_posted_at) over (partition by subscriber_id order by transaction_posted_at),
            {{ high_date }}
        ) as plan_valid_to
    from billing_events

),

subscriber_unknown as (

    -- guard fallback for a subscriber_sk join miss. Reads dim_subscriber's
    -- own unknown row instead of recomputing generate_surrogate_key(['-1'])
    -- here, so this fact can never drift from whatever key that dimension
    -- actually wrote.
    select subscriber_sk
    from {{ ref('dim_subscriber') }}
    where subscriber_id = '-1'

),

plan_unknown as (

    -- guard fallback for a plan_sk join miss, and the real fallback for
    -- every pre-first-billing-transaction day. Reads dim_plan's own
    -- unknown row (plan_sk, current_price_usd = 0.00, billing_period =
    -- 'Unknown') rather than recomputing it.
    select plan_sk, current_price_usd, billing_period
    from {{ ref('dim_plan') }}
    where plan_id = '-1'

)

select
    g.snapshot_date_key,
    coalesce(ds.subscriber_sk, su.subscriber_sk) as subscriber_sk,
    coalesce(dp.plan_sk, pu.plan_sk) as plan_sk,
    coalesce(ds.status, 'Unknown') as subscription_status,
    -- semi-additive MRR: monthly plan price as of that day, annual plans
    -- amortized to a monthly-equivalent, zero for trial and paused per
    -- modeling.md. billing_period = 'Unknown' (the pre-first-billing /
    -- join-miss fallback) falls through to the else branch, where
    -- current_price_usd is already 0.00 on dim_plan's unknown row, so no
    -- separate case arm is needed for that fallback.
    case
        when coalesce(ds.status, 'Unknown') in ('trial', 'paused') then cast(0.00 as decimal(10, 2))
        when coalesce(dp.billing_period, pu.billing_period) = 'annual'
            then cast(round(coalesce(dp.current_price_usd, pu.current_price_usd) / 12, 2) as decimal(10, 2))
        else cast(coalesce(dp.current_price_usd, pu.current_price_usd) as decimal(10, 2))
    end as mrr_amount_usd,
    g.tenure_days,
    coalesce(ds.status, 'Unknown') = 'trial' as is_in_trial,
    cast(current_timestamp as timestamp(6)) as loaded_at
from snapshot_grid as g
left join {{ ref('dim_subscriber') }} as ds
    on ds.subscriber_id = g.subscriber_id
    and g.snapshot_instant >= ds.effective_from
    and g.snapshot_instant < ds.effective_to
left join billing_intervals as bi
    on bi.subscriber_id = g.subscriber_id
    and g.snapshot_instant >= bi.plan_valid_from
    and g.snapshot_instant < bi.plan_valid_to
left join {{ ref('dim_plan') }} as dp
    on dp.plan_id = bi.plan_id
cross join subscriber_unknown as su
cross join plan_unknown as pu
