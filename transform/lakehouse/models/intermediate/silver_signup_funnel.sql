-- silver signup funnel. deduplicates upstream retry duplication on
-- signup_id: verified 70,701 bronze rows collapse to 70,000 distinct
-- signup_id values (701 ids duplicated, every duplicate group is exactly
-- size 2 with byte-identical _payload_hash across the pair, a true retry
-- replay rather than two different payloads racing for the same id).
--
-- tie-break rule: partition by signup_id, keep the row with the latest
-- _ingested_at, ties broken by the lowest _batch_id. because observed
-- duplicates are payload-identical, which physical copy survives the
-- tie-break is immaterial to the output, the rule only needs to be
-- deterministic, not discriminating.

with deduped as (
    select
        *,
        row_number() over (
            partition by signup_id
            order by _ingested_at desc, _batch_id asc
        ) as _dedup_rank
    from {{ ref('stg_signup_funnel') }}
)

select
    signup_id,
    subscriber_id,
    attempt_started_at,
    registered_at,
    email_verified_at,
    payment_method_added_at,
    selected_plan_id,
    plan_selected_at,
    first_stream_at,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from deduped
where _dedup_rank = 1
