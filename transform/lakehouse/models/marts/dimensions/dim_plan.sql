{{
    config(
        materialized='table'
    )
}}

-- dim_plan (Type 3, .notes/modeling.md). Grain: one row per plan,
-- permanently. Full-refresh table, not incremental: silver_plans carries
-- only current plan state (no price/tier history reaches silver, per the
-- source generator), so this build has no prior gold state to diff
-- against. previous_tier, previous_price_usd, tier_change_date, and
-- price_change_date are therefore structurally NULL here, not derived,
-- see the column descriptions in _dim_plan.yml and the entry in
-- .notes/decisions.md dated 2026-08-04. They will only start populating
-- once dim_plan is converted to an incremental model that diffs an
-- incoming silver row against this same model's previously materialized
-- row for that plan_id, a later work package's job.

with silver as (

    select
        plan_id,
        plan_name,
        current_tier,
        current_price_usd,
        billing_period,
        max_concurrent_streams,
        video_quality,
        has_ads,
        supports_downloads
    from {{ ref('silver_plans') }}

),

plans as (

    select
        {{ dbt_utils.generate_surrogate_key(['plan_id']) }} as plan_sk,
        plan_id,
        plan_name,
        current_tier,
        cast(null as varchar) as previous_tier,
        cast(null as date) as tier_change_date,
        current_price_usd,
        cast(null as decimal(8, 2)) as previous_price_usd,
        cast(null as date) as price_change_date,
        billing_period,
        max_concurrent_streams,
        video_quality,
        has_ads,
        supports_downloads
    from silver

),

unknown as (

    select
        {{ dbt_utils.generate_surrogate_key(["'-1'"]) }} as plan_sk,
        '-1' as plan_id,
        'Unknown' as plan_name,
        'Unknown' as current_tier,
        cast(null as varchar) as previous_tier,
        cast(null as date) as tier_change_date,
        cast(0 as decimal(8, 2)) as current_price_usd,
        cast(null as decimal(8, 2)) as previous_price_usd,
        cast(null as date) as price_change_date,
        'Unknown' as billing_period,
        0 as max_concurrent_streams,
        'Unknown' as video_quality,
        false as has_ads,
        false as supports_downloads

),

unioned as (

    select * from plans
    union all
    select * from unknown

)

select
    *,
    cast(current_timestamp as timestamp(6)) as loaded_at
from unioned
