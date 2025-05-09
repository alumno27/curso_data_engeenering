{{ config(materialized="view") }}

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast(dateadd(yy,2,current_date() ) as date)"
    ) }}
)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    to_char(date_day, 'YYYY-MM') as year_month,
    to_char(date_day, 'Month') as month_name,
    to_char(date_day, 'DY') as weekday_abbr,
    to_char(date_day, 'Day') as weekday_name,
    extract(dow from date_day) + 1 as day_of_week,
    date_trunc('week', date_day) as week_start,
    date_trunc('month', date_day) as month_start,
    date_trunc('quarter', date_day) as quarter_start,
    date_trunc('year', date_day) as year_start,
    case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend
from spine

order by date_day desc