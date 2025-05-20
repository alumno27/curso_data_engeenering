{{ 
  config(
    materialized = 'view'
  ) 
}}

with base as (
    select * 
    from {{ ref('base_sql_server_dbo__orders') }}
),

cleaned as (
    select
        order_id,
        user_id,
        promo_id,
        address_id,
        created_at,
        delivered_at,
        order_cost,
        shipping_cost,
        order_total,
        convert_timezone('UTC', synced_at)::timestamp_tz as synced_at_utc,
        _fivetran_deleted
    from base
    where coalesce(_fivetran_deleted, false) = false
)

select * from cleaned
