-- thin 1:1 staging model over the signup funnel accumulating-snapshot
-- source. bronze carries duplicate signup_id rows from upstream retries;
-- dedup happens in intermediate, not here.
--
-- plan_id_selected is renamed to selected_plan_id to read as an attribute
-- of the attempt rather than a second natural key.

select
    signup_id,
    subscriber_id,
    cast(attempt_started_at as timestamp(6)) as attempt_started_at,
    cast(registered_at as timestamp(6)) as registered_at,
    cast(email_verified_at as timestamp(6)) as email_verified_at,
    cast(payment_method_added_at as timestamp(6)) as payment_method_added_at,
    plan_id_selected as selected_plan_id,
    cast(plan_selected_at as timestamp(6)) as plan_selected_at,
    cast(first_stream_at as timestamp(6)) as first_stream_at,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ source('bronze', 'bronze_signup_funnel') }}
