{{
    config(
        materialized='table'
    )
}}

-- dim_payment_method (junk dimension, .notes/modeling.md). Grain: one row
-- per distinct combination of payment_type, is_promo_applied, is_retry,
-- is_autopay. A junk dimension collects several low-cardinality flag and
-- code columns that would otherwise sit directly on the fact table into
-- one small combination dimension, so fct_billing_transactions carries a
-- single payment_method_sk foreign key instead of four separate columns,
-- keeping the fact grain clean and letting these attributes be filtered,
-- grouped, and labeled the same way any other dimension is. There is no
-- natural key: the combination of the four attributes is the identity, so
-- two silver rows with the same four values are, by definition, the same
-- payment method row here.
--
-- Seeded as the full cross product of the domains observed in
-- silver_billing_ledger today (distinct payment_type values crossed with
-- both boolean states for is_promo_applied, is_retry, and is_autopay),
-- not merely the combinations that happen to co-occur on an existing
-- billing row. This makes every combination derivable up front, including
-- ones no transaction has posted yet, so a fact load can always resolve a
-- key without waiting on a new combination to first appear together in
-- silver. A future payment_type value appearing in silver will widen this
-- cross product on the next full-refresh build.

with observed_payment_types as (

    select distinct payment_type
    from {{ ref('silver_billing_ledger') }}

),

observed_promo_flags as (

    select distinct is_promo_applied
    from {{ ref('silver_billing_ledger') }}

),

observed_retry_flags as (

    select distinct is_retry
    from {{ ref('silver_billing_ledger') }}

),

observed_autopay_flags as (

    select distinct is_autopay
    from {{ ref('silver_billing_ledger') }}

),

cross_product as (

    select
        payment_type.payment_type,
        promo.is_promo_applied,
        retry.is_retry,
        autopay.is_autopay
    from observed_payment_types as payment_type
    cross join observed_promo_flags as promo
    cross join observed_retry_flags as retry
    cross join observed_autopay_flags as autopay

),

unknown_member as (

    -- exactly one unknown row for a billing transaction that cannot
    -- resolve a payment method. Should not occur at fact-build time since
    -- payment_type is not_null in silver, but every hash-keyed dimension
    -- carries this fallback per the project-wide rule (modeling.md,
    -- "Surrogate key strategy", unknown member). That general rule is
    -- written for dims with a natural key column ("surrogate key
    -- md5('-1'), natural key '-1'"), which this junk dimension does not
    -- have: "the combination is the identity" per its own section, so the
    -- unknown row's key is the same generate_surrogate_key call over this
    -- sentinel ('Unknown', false, false, false) combination as every other
    -- row gets, not a literal md5('-1'). Flagged in .notes/open-
    -- questions.md rather than assumed silently.
    select
        'Unknown' as payment_type,
        false as is_promo_applied,
        false as is_retry,
        false as is_autopay

),

unioned as (

    select * from cross_product
    union all
    select * from unknown_member

)

select
    -- dbt_utils.generate_surrogate_key casts each component to varchar
    -- before hashing (see dbt_packages/dbt_utils/macros/sql/
    -- generate_surrogate_key.sql), and Trino's cast(boolean as varchar)
    -- already produces lowercase 'true' / 'false', so passing the three
    -- flag columns straight through satisfies modeling.md's "booleans
    -- cast to 'true'/'false' text before hashing" without a redundant
    -- explicit case expression: verified directly in Trino,
    -- cast(true as varchar) = 'true', cast(false as varchar) = 'false'.
    {{ dbt_utils.generate_surrogate_key([
        'payment_type',
        'is_promo_applied',
        'is_retry',
        'is_autopay'
    ]) }} as payment_method_sk,
    payment_type,
    is_promo_applied,
    is_retry,
    is_autopay,
    cast({{ dbt.current_timestamp() }} as timestamp(6)) as loaded_at
from unioned
