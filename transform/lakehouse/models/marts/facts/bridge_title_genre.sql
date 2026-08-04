{{
    config(
        materialized='table'
    )
}}

-- bridge_title_genre (.notes/modeling.md). Grain: one row per (title_id,
-- genre_name) pair, allocation_weight summing to exactly 1.0000 per
-- title_id. This is a many-to-many bridge table: a title can carry more
-- than one genre (real data: 1 to 4 genres across 5,000 titles, 12,385
-- rows total), so genre cannot live as a plain attribute on dim_title
-- without either violating dim_title's one-row-per-metadata-version grain
-- or arbitrarily keeping only one genre and losing the rest. A bridge
-- table resolves the many-to-many relationship without duplicating
-- dim_title rows.
--
-- Keyed on title_id, dim_title's natural key, not title_sk, per
-- modeling.md: genre assignment is not itself history-tracked, so keying
-- on the natural key means a new dim_title SCD version never requires
-- re-emitting bridge rows. Queries reach this bridge from a fact through
-- dim_title: fact to dim_title on title_sk, then dim_title.title_id to
-- this bridge on title_id. There is no dim_genre in the approved plan;
-- genre_name is the identifier.
--
-- allocation_weight is passed through unchanged, not derived here:
-- verified against the real data that silver_title_genres already carries
-- a DECIMAL(6,4) allocation_weight column, and a direct grouped sum over
-- the whole table (not a sample) confirms it already totals exactly
-- 1.0000 per title_id with zero exceptions across all 5,000 titles.
--
-- The allocation_weight mechanism exists so a weighted fact query, one
-- that joins a fact to this bridge on title_id and multiplies its measure
-- by allocation_weight, does not double count: a title with three genre
-- rows would otherwise triple its contribution to any measure summed by
-- genre, since allocation_weight distributes exactly 1.0 of "this title"
-- across its genre rows. An unweighted "any exposure" query, for example
-- counting playback rows or summing a measure by genre through a plain
-- join with no weighting or deduplication, is a deliberate and known
-- double-counting risk for any title with more than one genre row: every
-- second watched, dollar billed, or watchlist add for that title is
-- counted once per genre it belongs to. That is inherent to a
-- many-to-many bridge, not a defect in this model; analysts must either
-- multiply by allocation_weight or explicitly deduplicate before treating
-- a bridge join as row-count-safe.
--
-- is_primary_genre is derived here per modeling.md's tie-break rule: true
-- on the row with the maximum allocation_weight per title_id, ties broken
-- by the alphabetically first genre_name. Real data has genuine ties to
-- break, not just a hypothetical: tt_03274 (documentary and fantasy, both
-- 0.3289) and tt_03380 (comedy and crime, both 0.3602), confirmed against
-- the real table before relying on row_number() to resolve them
-- deterministically.

with silver as (

    select
        title_id,
        genre_name,
        allocation_weight
    from {{ ref('silver_title_genres') }}

)

select
    title_id,
    genre_name,
    allocation_weight,
    row_number() over (
        partition by title_id
        order by allocation_weight desc, genre_name asc
    ) = 1 as is_primary_genre,
    cast(current_timestamp as timestamp(6)) as loaded_at
from silver
