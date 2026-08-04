-- silver billing ledger. deduplicates upstream retry duplication on
-- billing_transaction_id: verified 1,515,049 bronze rows collapse to
-- 1,500,100 distinct billing_transaction_id values (14,949 ids duplicated,
-- every duplicate group is exactly size 2 with byte-identical
-- _payload_hash across the pair, a true retry replay rather than two
-- different payloads racing for the same id).
--
-- tie-break rule: partition by billing_transaction_id, keep the row with
-- the latest _ingested_at, ties broken by the lowest _batch_id. because
-- observed duplicates are payload-identical, which physical copy survives
-- the tie-break is immaterial to the output, the rule only needs to be
-- deterministic, not discriminating.

with deduped as (
    select
        *,
        row_number() over (
            partition by billing_transaction_id
            order by _ingested_at desc, _batch_id asc
        ) as _dedup_rank
    from {{ ref('stg_billing_ledger') }}
)

select
    billing_transaction_id,
    subscriber_id,
    plan_id,
    payment_type,
    is_promo_applied,
    is_retry,
    is_autopay,
    transaction_type,
    transaction_posted_at,
    amount_usd,
    tax_amount_usd,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from deduped
where _dedup_rank = 1
