-- silver watchlist add events. no dedup pathology observed on
-- watchlist_event_id (verified: 750,000 rows, 750,000 distinct ids);
-- passthrough of the typed staging model. repeated (subscriber_id,
-- title_id) pairs are expected and valid here, a re-add after a removal
-- is a legitimate new event with its own watchlist_event_id.

select
    watchlist_event_id,
    subscriber_id,
    title_id,
    added_at,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ ref('stg_watchlist_adds') }}
