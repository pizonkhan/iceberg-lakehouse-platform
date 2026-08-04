-- thin 1:1 staging model over the watchlist add event stream.

select
    watchlist_event_id,
    subscriber_id,
    title_id,
    cast(added_at as timestamp(6)) as added_at,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ source('bronze', 'bronze_watchlist_adds') }}
