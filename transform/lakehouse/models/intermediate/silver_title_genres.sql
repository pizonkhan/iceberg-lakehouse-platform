-- silver title-genre association. no dedup pathology observed on
-- (title_id, genre_name) (verified: 12,385 rows, 12,385 distinct pairs);
-- passthrough of the typed staging model.

select
    title_id,
    genre_name,
    allocation_weight,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ ref('stg_title_genres') }}
