{{
    config(
        materialized='table'
    )
}}

-- dim_date (.notes/modeling.md). Grain: one row per calendar day,
-- 2023-01-01 through 2027-12-31. Generated with dbt_utils.date_spine, not a
-- static seed, per the work package: the physical range has to be checked
-- against real event data before it is fixed, not assumed. Verified
-- min/max event timestamps directly in Trino across silver_playback_sessions
-- (2023-01-01 to 2026-08-04), silver_billing_ledger (2023-01-01 to
-- 2026-08-03), silver_subscriber_events (2023-01-01 to 2026-08-03),
-- silver_watchlist_adds (2023-01-13 to 2026-08-03), and
-- silver_signup_funnel (2023-01-01 to 2026-08-04, across every milestone
-- timestamp column). Every one of those falls comfortably inside
-- 2023-01-01 through 2027-12-31, so the modeling.md range needed no
-- widening; see .notes/decisions.md for the full verification note.

with spine as (

    -- end_date is 2028-01-01, one day past the desired inclusive upper
    -- bound (2027-12-31), not a typo: date_spine's row count comes from
    -- dbt.get_intervals_between's date_diff(start_date, end_date), which
    -- is an exclusive day count, so passing the true end date undercounts
    -- the spine by one and silently drops the last calendar day. Verified
    -- directly against the real build: end_date='2027-12-31' produced
    -- 1,825 rows running only through 2027-12-30. Confirmed the fix by
    -- rebuilding with 2028-01-01: 1,826 rows, min 2023-01-01, max
    -- 2027-12-31, exactly the modeling.md grain. See .notes/decisions.md.
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2028-01-01' as date)"
    ) }}

),

calendar as (

    select
        cast(date_format(date_day, '%Y%m%d') as integer) as date_key,
        date_day,
        -- trino's day_of_week is already ISO-8601: 1 = Monday, 7 = Sunday.
        cast(day_of_week(date_day) as integer) as day_of_week,
        date_format(date_day, '%W') as day_name,
        cast(day(date_day) as integer) as day_of_month,
        cast(day_of_year(date_day) as integer) as day_of_year,
        cast(week(date_day) as integer) as iso_week_of_year,
        cast(month(date_day) as integer) as month_of_year,
        date_format(date_day, '%M') as month_name,
        cast(quarter(date_day) as integer) as quarter_of_year,
        cast(year(date_day) as integer) as year_number,
        day_of_week(date_day) in (6, 7) as is_weekend,
        day(date_day) = 1 as is_month_start,
        date_day = last_day_of_month(date_day) as is_month_end
    from spine

),

fiscal as (

    select
        *,
        -- fiscal year starts july 1, fiscal_month 1 = july. shift the
        -- calendar month back by 6, wrap mod 12, restore 1-based numbering.
        -- month_of_year=7 (jul) -> 1, month_of_year=6 (jun) -> 12,
        -- month_of_year=1 (jan) -> 7.
        cast(mod(month_of_year + 5, 12) + 1 as integer) as fiscal_month
    from calendar

),

fiscal_periods as (

    select
        *,
        -- three fiscal months per quarter, 1-based: fiscal_month 1-3 -> Q1
        -- (jul-sep) ... 10-12 -> Q4 (apr-jun).
        cast(((fiscal_month - 1) / 3) + 1 as integer) as fiscal_quarter,
        -- labeled by the ending calendar year (fy2026 = 2025-07-01 through
        -- 2026-06-30): any date in july through december belongs to the
        -- fiscal year that closes the following june, everything
        -- january-june belongs to the fiscal year closing that same june.
        cast(
            case
                when month_of_year >= 7 then year_number + 1
                else year_number
            end as integer
        ) as fiscal_year
    from fiscal

),

holidays as (

    select
        *,
        -- us federal holidays (5 u.s.c. 6103), flagged on their actual
        -- calendar date, not the federal-employee "observed" shift when a
        -- fixed date lands on a weekend (that shift is a payroll
        -- convention, not a property of the calendar date itself). fixed
        -- dates: new year's, juneteenth, independence day, veterans day,
        -- christmas. nth-weekday-of-month dates use the standard
        -- floor((day - 1) / 7) + 1 occurrence formula, already scoped to
        -- the correct day_of_week by the surrounding case condition.
        -- memorial day (last monday of may) is identified by checking the
        -- following monday rolls into june.
        case
            when month_of_year = 1 and day_of_month = 1
                then 'New Year''s Day'
            when month_of_year = 1 and day_of_week = 1
                and ((day_of_month - 1) / 7) + 1 = 3
                then 'Martin Luther King Jr. Day'
            when month_of_year = 2 and day_of_week = 1
                and ((day_of_month - 1) / 7) + 1 = 3
                then 'Washington''s Birthday'
            when month_of_year = 5 and day_of_week = 1
                and month(date_day + interval '7' day) = 6
                then 'Memorial Day'
            when month_of_year = 6 and day_of_month = 19
                then 'Juneteenth National Independence Day'
            when month_of_year = 7 and day_of_month = 4
                then 'Independence Day'
            when month_of_year = 9 and day_of_week = 1
                and ((day_of_month - 1) / 7) + 1 = 1
                then 'Labor Day'
            when month_of_year = 10 and day_of_week = 1
                and ((day_of_month - 1) / 7) + 1 = 2
                then 'Columbus Day'
            when month_of_year = 11 and day_of_month = 11
                then 'Veterans Day'
            when month_of_year = 11 and day_of_week = 4
                and ((day_of_month - 1) / 7) + 1 = 4
                then 'Thanksgiving Day'
            when month_of_year = 12 and day_of_month = 25
                then 'Christmas Day'
        end as holiday_name
    from fiscal_periods

)

select
    date_key,
    date_day,
    day_of_week,
    day_name,
    day_of_month,
    day_of_year,
    iso_week_of_year,
    month_of_year,
    month_name,
    quarter_of_year,
    year_number,
    fiscal_month,
    fiscal_quarter,
    fiscal_year,
    is_weekend,
    holiday_name is not null as is_holiday,
    holiday_name,
    is_month_start,
    is_month_end,
    cast({{ dbt.current_timestamp() }} as timestamp(6)) as loaded_at
from holidays
