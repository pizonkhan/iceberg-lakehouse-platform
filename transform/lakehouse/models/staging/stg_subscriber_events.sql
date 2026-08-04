-- thin 1:1 staging model over the subscriber change-event stream.
-- one row per change event, exactly as bronze has it: no dedup, no
-- collapsing to latest-state, that logic belongs to intermediate and to
-- gold's SCD build respectively.

select
    subscriber_id,
    change_event_id,
    cast(change_timestamp as timestamp(6)) as changed_at,
    change_type,
    email,
    display_name,
    country_code,
    acquisition_channel,
    signup_date,
    plan_tier,
    status,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ source('bronze', 'bronze_subscriber_events') }}
