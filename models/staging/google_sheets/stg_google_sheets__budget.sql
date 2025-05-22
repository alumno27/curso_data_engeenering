{{ config(
    materialized = 'view'
) }}

with base as (
    select *
    from {{ ref('base_google_sheets__budget_') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id', 'month']) }} as budget_id,
        budget,
        product_id,
        month,
        quantity,
        convert_timezone('UTC', synced_at)::timestamp_tz as synced_at
        
    from base
)

select * from cleaned
