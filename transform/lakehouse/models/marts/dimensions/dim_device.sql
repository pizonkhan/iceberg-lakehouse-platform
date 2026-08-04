-- dim_device, Type 1 (see .notes/modeling.md). Grain: one row per device,
-- current state only, corrections overwrite in place. No SCD tracking
-- columns: silver_devices is static reference data with no versioning to
-- preserve. Full refresh every build.

with silver_devices as (

    select
        device_id,
        device_type,
        manufacturer,
        model_name,
        os_name
    from {{ ref('silver_devices') }}

),

-- exactly one unknown member row, natural key '-1', so a fact that cannot
-- resolve a device still gets a valid, joinable device_sk (see
-- .notes/modeling.md, "Surrogate key strategy", unknown member).
unknown_member as (

    select
        '-1' as device_id,
        'Unknown' as device_type,
        'Unknown' as manufacturer,
        'Unknown' as model_name,
        'Unknown' as os_name

),

unioned as (

    select * from silver_devices
    union all
    select * from unknown_member

)

select
    -- single-component key: dbt_utils.generate_surrogate_key(['device_id'])
    -- reduces to md5(device_id) for a one-field list, matching the
    -- contract exactly while keeping key generation on the shared macro.
    {{ dbt_utils.generate_surrogate_key(['device_id']) }} as device_sk,
    device_id,
    device_type,
    manufacturer,
    model_name,
    os_name,
    -- derived independently from device_type rather than passed through
    -- from silver_devices.is_mobile, so this dimension stays self
    -- consistent with its own contract even if silver's column ever
    -- drifts. see .notes/decisions.md.
    device_type in ('mobile', 'tablet') as is_mobile,
    cast({{ dbt.current_timestamp() }} as timestamp(6)) as loaded_at
from unioned
