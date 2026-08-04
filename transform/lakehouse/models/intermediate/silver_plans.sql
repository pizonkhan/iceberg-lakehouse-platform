-- silver plan reference. no dedup pathology observed on plan_id
-- (verified: 30 rows, 30 distinct ids); passthrough of the typed staging
-- model.

select
    plan_id,
    plan_name,
    current_tier,
    current_price_usd,
    billing_period,
    max_concurrent_streams,
    video_quality,
    has_ads,
    supports_downloads,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ ref('stg_plans') }}
