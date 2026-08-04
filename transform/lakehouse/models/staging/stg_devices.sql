-- thin 1:1 staging model over the static device reference table.

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
from {{ source('bronze', 'bronze_devices') }}
