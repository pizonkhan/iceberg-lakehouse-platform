-- silver title metadata change-event stream. cleaning only: dedup any
-- exact-duplicate ingested change event, type-cast passthrough. this is
-- NOT a "latest state" table, one row per change event survives, same as
-- bronze and staging. gold's dim_title Type 2 build reads this and does
-- the SCD versioning; doing that here would be a layer violation.
--
-- dedup tie-break: same rule as silver_subscriber_events, see that model
-- for the rationale. no duplicate change_event_id values exist in the
-- current bronze data (verified: 15,098 rows, 15,098 distinct ids); this
-- is a defensive no-op today.

with deduped as (
    select
        *,
        row_number() over (
            partition by change_event_id
            order by _ingested_at desc, _batch_id asc
        ) as _dedup_rank
    from {{ ref('stg_title_events') }}
)

select
    title_id,
    change_event_id,
    changed_at,
    change_type,
    title_name,
    content_type,
    release_year,
    runtime_minutes,
    maturity_rating,
    original_language,
    is_original,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from deduped
where _dedup_rank = 1
