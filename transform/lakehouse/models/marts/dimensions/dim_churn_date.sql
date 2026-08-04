{{
    config(
        materialized='view'
    )
}}

-- role-playing date dimension over dim_date (.notes/modeling.md), the
-- "churn" role: FK target for dim_subscriber.churn_date_key. Unlike the
-- other two role views this one has no fact-grain consumer at all; it
-- exists so an attribute on a dimension (dim_subscriber.churn_date_key) can
-- still resolve through a proper role-playing date dimension instead of a
-- raw DATE column, per modeling.md's note on dim_subscriber. A genuine
-- view, not a copy: marts/dimensions defaults to materialized='table' in
-- dbt_project.yml, overridden here on purpose. Every column passes through
-- unchanged except the two renamed to carry the role in their name.

select
    date_key as churn_date_key,
    date_day as churn_date,
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
