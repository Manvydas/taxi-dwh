with source as (
    select
        id,
        full_date,
        year,
        year_week,
        year_day,
        fiscal_year,
        fiscal_qtr,
        month,
        month_name,
        week_day, -- 0-6
        day_name,
        day_is_weekday,
    from {{ ref('dim_date') }}
)
select *
from source