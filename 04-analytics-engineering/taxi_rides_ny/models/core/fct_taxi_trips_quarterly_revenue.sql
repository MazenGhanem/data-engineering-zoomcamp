{{ config(materialized='table') }}

with base as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        format('%d/Q%d', extract(year from pickup_datetime), extract(quarter from pickup_datetime)) as year_quarter,
        sum(total_amount) as revenue
    from {{ ref('fact_trips') }}
    group by 1, 2, 3, 4
),

yoy_calc as (
    select
        b.*,
        lag(revenue) over (
            partition by service_type, quarter 
            order by year
        ) as prev_year_revenue
    from base b
)

select
    service_type,
    year,
    quarter,
    year_quarter,
    revenue,
    prev_year_revenue,
    case 
        when prev_year_revenue is null or prev_year_revenue = 0 then null
        else round((revenue - prev_year_revenue) / prev_year_revenue * 100, 2)
    end as yoy_growth_pct
from yoy_calc
order by service_type, year, quarter