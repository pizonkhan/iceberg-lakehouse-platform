{{
    config(
        materialized='table'
    )
}}

-- fct_billing_transactions (transaction fact, .notes/modeling.md). Grain:
-- one row per discrete billing ledger event (charge, refund, credit, or
-- proration) posted to one subscriber's account. billing_transaction_id
-- is the degenerate dimension and unique merge key; silver_billing_ledger
-- is already deduplicated on this id by the silver work package
-- (1,500,100 distinct ids, see .notes/decisions.md 2026-08-04 dedup
-- entry), so this build does not re-dedup.
--
-- subscriber_sk resolves via the literal point-in-time interval-overlap
-- predicate modeling.md's "Point-in-time join rule" section gives for
-- this exact fact (transaction_posted_at against dim_subscriber's
-- half-open [effective_from, effective_to)), implemented verbatim below,
-- falling back to the unknown member only as a guard. The fallback is
-- read from dim_subscriber's own unknown row rather than recomputed by
-- hand here, so this fact can never drift from whatever key that
-- dimension actually wrote.
--
-- plan_sk is a plain natural-key FK to dim_plan (dim_plan is Type 3, not
-- versioned, so no point-in-time join is needed or possible).
--
-- payment_method_sk resolves by joining the four raw junk-dimension
-- columns to dim_payment_method's pre-enumerated combination, not by
-- recomputing the hash: dim_payment_method was seeded as the full cross
-- product of the domain observed in this same silver table.
--
-- billing_date_key resolves through the dim_billing_date role-playing
-- view (not the base dim_date), joined on the calendar date component of
-- transaction_posted_at, per modeling.md's role-playing dimension
-- requirement.
--
-- Sign convention (modeling.md: "charges positive, refunds and credits
-- negative, prorations either sign") is a straight passthrough of
-- amount_usd and tax_amount_usd, not derived or corrected: verified
-- against the real data before writing this model that
-- silver_billing_ledger already encodes the convention exactly as
-- specified, with zero exceptions across all 1,500,100 rows (charge
-- always > 0, refund and credit always < 0, proration both signs). See
-- .notes/decisions.md for the full grouped min/max check.

with silver as (

    select
        billing_transaction_id,
        subscriber_id,
        plan_id,
        payment_type,
        is_promo_applied,
        is_retry,
        is_autopay,
        transaction_type,
        transaction_posted_at,
        amount_usd,
        tax_amount_usd
    from {{ ref('silver_billing_ledger') }}

),

subscriber_unknown as (

    -- guard fallback for a subscriber_sk join miss. Reads dim_subscriber's
    -- own unknown row instead of recomputing generate_surrogate_key(['-1'])
    -- here.
    select subscriber_sk
    from {{ ref('dim_subscriber') }}
    where subscriber_id = '-1'

),

plan_unknown as (

    select plan_sk
    from {{ ref('dim_plan') }}
    where plan_id = '-1'

),

payment_method_unknown as (

    -- dim_payment_method has no natural key column, so its unknown row's
    -- key is not md5('-1'); payment_type = 'Unknown' is the only row with
    -- that value and uniquely identifies it (see dim_payment_method.sql).
    select payment_method_sk
    from {{ ref('dim_payment_method') }}
    where payment_type = 'Unknown'

)

select
    f.billing_transaction_id,
    coalesce(ds.subscriber_sk, su.subscriber_sk) as subscriber_sk,
    coalesce(dp.plan_sk, pu.plan_sk) as plan_sk,
    coalesce(dpm.payment_method_sk, pmu.payment_method_sk) as payment_method_sk,
    dbd.billing_date_key,
    f.transaction_posted_at,
    f.transaction_type,
    f.amount_usd,
    f.tax_amount_usd,
    cast(current_timestamp as timestamp(6)) as loaded_at
from silver as f
left join {{ ref('dim_subscriber') }} as ds
    on ds.subscriber_id = f.subscriber_id
    and f.transaction_posted_at >= ds.effective_from
    and f.transaction_posted_at < ds.effective_to
left join {{ ref('dim_plan') }} as dp
    on dp.plan_id = f.plan_id
left join {{ ref('dim_payment_method') }} as dpm
    on dpm.payment_type = f.payment_type
    and dpm.is_promo_applied = f.is_promo_applied
    and dpm.is_retry = f.is_retry
    and dpm.is_autopay = f.is_autopay
left join {{ ref('dim_billing_date') }} as dbd
    on dbd.billing_date = cast(f.transaction_posted_at as date)
cross join subscriber_unknown as su
cross join plan_unknown as pu
cross join payment_method_unknown as pmu
