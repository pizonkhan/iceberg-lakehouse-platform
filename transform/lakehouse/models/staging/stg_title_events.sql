-- thin 1:1 staging model over the title metadata change-event stream.
-- one row per change event, exactly as bronze has it: no dedup, no
-- collapsing to latest-state, that logic belongs to intermediate and to
-- gold's SCD build respectively.

select
    title_id,
    change_event_id,
    cast(change_timestamp as timestamp(6)) as changed_at,
    change_type,
    title_name,
    content_type,
    cast(release_year as integer) as release_year,
    cast(runtime_minutes as integer) as runtime_minutes,
    maturity_rating,
    original_language,
    is_original,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ source('bronze', 'bronze_title_events') }}
