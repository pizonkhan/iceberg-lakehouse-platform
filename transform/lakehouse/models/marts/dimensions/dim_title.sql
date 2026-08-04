-- dim_title: pure Type 2 SCD over title metadata. Grain is one row per
-- title per metadata version, where a version begins whenever any of the
-- seven tracked attributes changes (title_name, content_type,
-- release_year, runtime_minutes, maturity_rating, original_language,
-- is_original). Full-refresh table, not incremental: at ~15k rows the
-- incremental machinery buys nothing and only adds surface area, see
-- decisions.md. No is_inferred column and no late-arriving handling:
-- titles arrive from a controlled catalog feed ahead of playback, so
-- there is no self-heal step here (see modeling.md).
--
-- Full column-level contract: .notes/modeling.md, "dim_title (Type 2)".

with source as (

    select
        title_id,
        change_event_id,
        changed_at,
        title_name,
        content_type,
        release_year,
        runtime_minutes,
        maturity_rating,
        original_language,
        is_original,
        _batch_id
    from {{ ref('silver_title_events') }}

),

-- tie-break for multiple events landing at the identical microsecond for
-- the same title: order by (_batch_id, change_event_id) and keep only
-- the last, so a same-instant collision never produces a zero-width
-- [t, t) interval. silver carries no separate "row sequence within the
-- batch" column; change_event_id stands in for it because it is assigned
-- monotonically at ingestion (ttev_00000000, ttev_00000001, ...), see
-- decisions.md.
deduped as (

    select
        *,
        row_number() over (
            partition by title_id, changed_at
            order by _batch_id desc, change_event_id desc
        ) as _tie_rank
    from source

),

tracked as (

    select
        title_id,
        changed_at,
        title_name,
        content_type,
        release_year,
        runtime_minutes,
        maturity_rating,
        original_language,
        is_original,
        {{ dbt_utils.generate_surrogate_key([
            'title_name',
            'content_type',
            'release_year',
            'runtime_minutes',
            'maturity_rating',
            'original_language',
            'is_original'
        ]) }} as row_hash
    from deduped
    where _tie_rank = 1

),

-- change detection: a version boundary opens only where row_hash differs
-- from the immediately preceding event for the same title. comparing
-- each event only to its immediate predecessor in changed_at order is
-- equivalent to comparing to the currently open version's hash: every
-- event collapsed away here shares its predecessor's hash by
-- construction, so the comparison telescopes back to the last retained
-- version (see modeling.md, "change detection").
changes as (

    select
        title_id,
        changed_at,
        title_name,
        content_type,
        release_year,
        runtime_minutes,
        maturity_rating,
        original_language,
        is_original,
        row_hash,
        lag(row_hash) over (
            partition by title_id order by changed_at
        ) as _prev_row_hash
    from tracked

),

versions as (

    select
        title_id,
        changed_at as effective_from,
        title_name,
        content_type,
        release_year,
        runtime_minutes,
        maturity_rating,
        original_language,
        is_original,
        row_hash
    from changes
    where _prev_row_hash is null or row_hash <> _prev_row_hash

),

scd as (

    select
        title_id,
        effective_from,
        coalesce(
            lead(effective_from) over (partition by title_id order by effective_from),
            timestamp '9999-12-31 23:59:59.999999'
        ) as effective_to,
        row_number() over (partition by title_id order by effective_from) as scd_version,
        title_name,
        content_type,
        release_year,
        runtime_minutes,
        maturity_rating,
        original_language,
        is_original,
        row_hash
    from versions

),

history as (

    select
        {{ dbt_utils.generate_surrogate_key(['title_id', 'effective_from']) }} as title_sk,
        title_id,
        title_name,
        content_type,
        release_year,
        runtime_minutes,
        maturity_rating,
        original_language,
        is_original,
        effective_from,
        effective_to,
        effective_to = timestamp '9999-12-31 23:59:59.999999' as is_current,
        scd_version,
        row_hash,
        cast(current_timestamp as timestamp(6)) as loaded_at
    from scd

),

-- unknown member: exactly one sentinel row so every fact FK can resolve.
-- spans the full [1900-01-01, high date) interval and is always current,
-- per modeling.md's "Unknown member" rule.
unknown as (

    select
        {{ dbt_utils.generate_surrogate_key(["'-1'"]) }} as title_sk,
        cast('-1' as varchar) as title_id,
        cast('Unknown' as varchar) as title_name,
        cast('Unknown' as varchar) as content_type,
        cast(null as integer) as release_year,
        cast(null as integer) as runtime_minutes,
        cast('Unknown' as varchar) as maturity_rating,
        cast('Unknown' as varchar) as original_language,
        false as is_original,
        timestamp '1900-01-01 00:00:00.000000' as effective_from,
        timestamp '9999-12-31 23:59:59.999999' as effective_to,
        true as is_current,
        1 as scd_version,
        {{ dbt_utils.generate_surrogate_key([
            "cast('Unknown' as varchar)",
            "cast('Unknown' as varchar)",
            "cast(null as integer)",
            "cast(null as integer)",
            "cast('Unknown' as varchar)",
            "cast('Unknown' as varchar)",
            "false"
        ]) }} as row_hash,
        cast(current_timestamp as timestamp(6)) as loaded_at

)

select * from history
union all
select * from unknown
