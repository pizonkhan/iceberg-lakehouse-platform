-- thin 1:1 staging model over the static plan reference table.

select
    plan_id,
    plan_name,
    current_tier,
    cast(current_price_usd as decimal(8, 2)) as current_price_usd,
    billing_period,
    cast(max_concurrent_streams as integer) as max_concurrent_streams,
    video_quality,
    has_ads,
    supports_downloads,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ source('bronze', 'bronze_plans') }}
