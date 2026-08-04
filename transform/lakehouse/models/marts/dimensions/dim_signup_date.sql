{{
    config(
        materialized='view'
    )
}}

-- role-playing date dimension over dim_date (.notes/modeling.md), the
-- "signup" role: FK target for fct_signup_funnel.signup_date_key. A genuine
-- view, not a copy, so it always reflects dim_date's current physical rows
-- with zero storage and zero refresh lag; marts/dimensions defaults to
-- materialized='table' in dbt_project.yml, overridden here on purpose.
-- Every column passes through unchanged except the two renamed to carry
-- the role in their name, per modeling.md's role-playing convention.

select
    date_key as signup_date_key,
    date_day as signup_date,
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
    is_holiday,
    holiday_name,
    is_month_start,
    is_month_end,
    loaded_at
from {{ ref('dim_date') }}
