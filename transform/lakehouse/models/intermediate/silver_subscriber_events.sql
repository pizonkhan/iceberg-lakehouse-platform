-- silver subscriber change-event stream. cleaning only: dedup any
-- exact-duplicate ingested change event, type-cast passthrough. this is
-- NOT a "latest state" table, one row per change event survives, same as
-- bronze and staging. gold's dim_subscriber Type 6 build reads this and
-- does the SCD versioning; doing that here would be a layer violation.
--
-- dedup tie-break: partition by change_event_id (the grain key of the
-- stream), keep the row with the latest _ingested_at, ties broken by the
-- lowest _batch_id. no duplicate change_event_id values exist in the
-- current bronze data (verified: 199,928 rows, 199,928 distinct ids), so
-- this is a defensive no-op today, kept because the source system's retry
-- behavior that duplicates signup_id and billing_transaction_id could
-- just as easily duplicate a change event on a future load.

with deduped as (
    select
        *,
        row_number() over (
            partition by change_event_id
            order by _ingested_at desc, _batch_id asc
        ) as _dedup_rank
    from {{ ref('stg_subscriber_events') }}
)

select
    subscriber_id,
    change_event_id,
    changed_at,
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
from deduped
where _dedup_rank = 1
