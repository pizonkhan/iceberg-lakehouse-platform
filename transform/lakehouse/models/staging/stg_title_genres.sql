-- thin 1:1 staging model over the title-genre weighted association table.

select
    title_id,
    genre_name,
    cast(allocation_weight as decimal(6, 4)) as allocation_weight,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ source('bronze', 'bronze_title_genres') }}
