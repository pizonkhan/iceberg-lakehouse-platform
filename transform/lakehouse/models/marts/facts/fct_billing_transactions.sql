{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='billing_transaction_id',
        on_schema_change='fail'
    )
}}

-- fct_billing_transactions (transaction fact, .notes/modeling.md). Grain:
-- one row per discrete billing ledger event (charge, refund, credit, or
-- proration) posted to one subscriber's account. billing_transaction_id
-- is the degenerate dimension and unique merge key; silver_billing_ledger
-- is already deduplicated on this id by the silver work package
-- (1,500,100 distinct ids, see .notes/decisions.md 2026-08-04 dedup
-- entry), so this build does not re-dedup.
--
-- Incremental via Iceberg MERGE (incremental_strategy='merge'), converted
-- from the original full-refresh table. The watermark is bronze ingestion
-- metadata (_ingested_at), not transaction_posted_at. silver_billing_ledger
-- is itself rebuilt full-refresh on every run, but it passes bronze's
-- original _ingested_at through untouched per row (see
-- silver_billing_ledger.sql), so "_ingested_at > max(_ingested_at) already
-- in this table" correctly identifies rows genuinely new to this run
-- regardless of how old their transaction_posted_at is. A watermark on
-- transaction_posted_at would silently drop any late-arriving billing
-- event with an old posted date, since a truly late arrival is exactly a
-- row whose event time is old but whose ingestion time is new; filtering
-- on the event time would filter it straight back out. _ingested_at
-- cannot make that mistake because it does not look at the event time at
-- all.
--
-- Backfill: to reprocess a specific _ingested_at range through this same
-- MERGE logic (for example, to re-pull a batch that was missed or
-- corrected upstream) without dropping or rebuilding the table, run:
--   dbt build --select fct_billing_transactions --target trino \
--     --vars '{backfill_start: "2026-08-04 00:00:00.000000", backfill_end: "2026-08-05 00:00:00.000000"}'
-- backfill_start and backfill_end are both optional and independently
-- omittable (an omitted bound is open on that side); when either is set,
-- it entirely replaces the normal watermark filter below for that run,
-- it does not narrow it further. Do not pass --full-refresh for a
-- backfill: --full-refresh drops and rebuilds the whole table from
-- scratch, which is the thing this mechanism exists to avoid.
--
-- subscriber_sk resolves via the literal point-in-time interval-overlap
-- predicate modeling.md's "Point-in-time join rule" section gives for
-- this exact fact (transaction_posted_at against dim_subscriber's
-- half-open [effective_from, effective_to)), implemented verbatim below,
-- falling back to the unknown member only as a guard. The fallback is
-- read from dim_subscriber's own unknown row rather than recomputed by
-- hand here, so this fact can never drift from whatever key that
-- dimension actually wrote.
--
-- plan_sk is a plain natural-key FK to dim_plan (dim_plan is Type 3, not
-- versioned, so no point-in-time join is needed or possible).
--
-- payment_method_sk resolves by joining the four raw junk-dimension
-- columns to dim_payment_method's pre-enumerated combination, not by
-- recomputing the hash: dim_payment_method was seeded as the full cross
-- product of the domain observed in this same silver table.
--
-- billing_date_key resolves through the dim_billing_date role-playing
-- view (not the base dim_date), joined on the calendar date component of
-- transaction_posted_at, per modeling.md's role-playing dimension
-- requirement.
--
-- Sign convention (modeling.md: "charges positive, refunds and credits
-- negative, prorations either sign") is a straight passthrough of
-- amount_usd and tax_amount_usd, not derived or corrected: verified
-- against the real data before writing this model that
-- silver_billing_ledger already encodes the convention exactly as
-- specified, with zero exceptions across all 1,500,100 rows (charge
-- always > 0, refund and credit always < 0, proration both signs). See
-- .notes/decisions.md for the full grouped min/max check.
--
-- Watermark implementation note: Trino rejects a plain "where _ingested_at
-- > (select max(_ingested_at) from {{ this }})" scalar subquery here with
-- "Given correlated subquery is not supported", because dbt-trino's merge
-- strategy compiles the incremental filter into a view that becomes the
-- MERGE statement's USING source, and that source ends up subquerying its
-- own MERGE target (confirmed directly against this project's Trino,
-- see .notes/decisions.md). The fix used below is the standard workaround
-- for this class of engine limitation: resolve the watermark to a plain
-- literal with a `run_query` pre-query before the main SQL is assembled,
-- so the compiled MERGE source never references the target table at all.
--
-- The gold fact table itself does not persist _ingested_at (it is bronze/
-- silver bookkeeping, not part of this fact's column contract, and this
-- work package changes materialization and filtering only, never the
-- contract), so the pre-query below recovers the watermark by joining
-- silver back to the target on the shared grain key: the maximum
-- _ingested_at among silver rows whose billing_transaction_id is already
-- present in {{ this }}. Because silver_billing_ledger always carries
-- every row's original _ingested_at (rebuilt full-refresh but never
-- altering that value) and MERGE only ever adds or updates rows matched
-- by billing_transaction_id, this join-based maximum is exactly the
-- ingestion-time watermark of the last successfully merged batch, with no
-- need for a hidden extra column on the fact.

{% set max_ingested_at_literal = '1900-01-01 00:00:00.000000' %}
{% if is_incremental() and var('backfill_start', none) is none and var('backfill_end', none) is none %}
    {% set get_max_ingested_at_query %}
        select coalesce(max(sbl._ingested_at), timestamp '1900-01-01 00:00:00.000000') as max_ingested_at
        from {{ ref('silver_billing_ledger') }} as sbl
        inner join {{ this }} as f
            on f.billing_transaction_id = sbl.billing_transaction_id
    {% endset %}
    {% if execute %}
        {% set max_ingested_at_value = run_query(get_max_ingested_at_query).columns['max_ingested_at'].values()[0] %}
        {% set max_ingested_at_literal = max_ingested_at_value.strftime('%Y-%m-%d %H:%M:%S.%f') %}
    {% endif %}
{% endif %}

with silver as (

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
        _ingested_at
    from {{ ref('silver_billing_ledger') }}

    {% if is_incremental() %}
    {% if var('backfill_start', none) is not none or var('backfill_end', none) is not none %}
    -- backfill mode: explicit _ingested_at window replaces the normal
    -- watermark entirely, see the invocation note above.
    where _ingested_at >= {{ "timestamp '" ~ var('backfill_start', '1900-01-01 00:00:00.000000') ~ "'" }}
      and _ingested_at <  {{ "timestamp '" ~ var('backfill_end', '9999-12-31 23:59:59.999999') ~ "'" }}
    {% else %}
    -- normal watermark: bronze ingestion time, not event time, see the
    -- header comment above for why. max_ingested_at_literal already
    -- defaults to a sentinel low timestamp when {{ this }} has no rows
    -- yet (coalesce runs inside the pre-query above).
    where _ingested_at > timestamp '{{ max_ingested_at_literal }}'
    {% endif %}
    {% endif %}

),

subscriber_unknown as (

    -- guard fallback for a subscriber_sk join miss. Reads dim_subscriber's
    -- own unknown row instead of recomputing generate_surrogate_key(['-1'])
    -- here.
    select subscriber_sk
    from {{ ref('dim_subscriber') }}
    where subscriber_id = '-1'

),

plan_unknown as (

    select plan_sk
    from {{ ref('dim_plan') }}
    where plan_id = '-1'

),

payment_method_unknown as (

    -- dim_payment_method has no natural key column, so its unknown row's
    -- key is not md5('-1'); payment_type = 'Unknown' is the only row with
    -- that value and uniquely identifies it (see dim_payment_method.sql).
    select payment_method_sk
    from {{ ref('dim_payment_method') }}
    where payment_type = 'Unknown'

)

select
    f.billing_transaction_id,
    coalesce(ds.subscriber_sk, su.subscriber_sk) as subscriber_sk,
    coalesce(dp.plan_sk, pu.plan_sk) as plan_sk,
    coalesce(dpm.payment_method_sk, pmu.payment_method_sk) as payment_method_sk,
    dbd.billing_date_key,
    f.transaction_posted_at,
    f.transaction_type,
    f.amount_usd,
    f.tax_amount_usd,
    cast(current_timestamp as timestamp(6)) as loaded_at
from silver as f
left join {{ ref('dim_subscriber') }} as ds
    on ds.subscriber_id = f.subscriber_id
    and f.transaction_posted_at >= ds.effective_from
    and f.transaction_posted_at < ds.effective_to
left join {{ ref('dim_plan') }} as dp
    on dp.plan_id = f.plan_id
left join {{ ref('dim_payment_method') }} as dpm
    on dpm.payment_type = f.payment_type
    and dpm.is_promo_applied = f.is_promo_applied
    and dpm.is_retry = f.is_retry
    and dpm.is_autopay = f.is_autopay
left join {{ ref('dim_billing_date') }} as dbd
    on dbd.billing_date = cast(f.transaction_posted_at as date)
cross join subscriber_unknown as su
cross join plan_unknown as pu
cross join payment_method_unknown as pmu
