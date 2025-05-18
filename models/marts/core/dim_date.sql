{{ config(
    materialized = 'table',
    tags = ['dimension']
) }}

-- 🗓 Generación de fechas con date_spine
with calendar as (
    select 
        date_day
    from {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2021-01-01' as date)",
    end_date="cast('2026-01-01' as date)"
   )
}}
),

-- 📅 Enriquecimiento con atributos temporales
enriched as (
    select
        date_day as date,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(quarter from date_day) as quarter,
        extract(week from date_day) as week,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Month') as month_name,
        case when extract(dow from date_day) in (0,6) then true else false end as is_weekend
    from calendar
)

select * from enriched
