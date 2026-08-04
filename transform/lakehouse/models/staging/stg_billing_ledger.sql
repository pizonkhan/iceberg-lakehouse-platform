-- thin 1:1 staging model over the billing ledger. bronze carries duplicate
-- billing_transaction_id rows from upstream retries; dedup happens in
-- intermediate, not here.

select
    billing_transaction_id,
    subscriber_id,
    plan_id,
    payment_type,
    is_promo_applied,
    is_retry,
    is_autopay,
    transaction_type,
    cast(transaction_posted_at as timestamp(6)) as transaction_posted_at,
    cast(amount_usd as decimal(10, 2)) as amount_usd,
    cast(tax_amount_usd as decimal(10, 2)) as tax_amount_usd,
    _source_file,
    _ingested_at,
    _batch_id,
    _payload_hash
from {{ source('bronze', 'bronze_billing_ledger') }}
