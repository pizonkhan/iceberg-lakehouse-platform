-- silver device reference. no dedup pathology observed on device_id
-- (verified: 3,000 rows, 3,000 distinct ids); passthrough of the typed
-- staging model.

select
    device_id,
    device_type,
    manufacturer,
    model_name,
    os_name,
    is_mobile,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ ref('stg_devices') }}
