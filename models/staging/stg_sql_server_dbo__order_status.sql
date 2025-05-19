{{ 
  config(
    materialized = 'view',
    
    tags         = ['staging']
  ) 
}}

with base as (
  select * from {{ ref('base_sql_server_dbo__orders') }}
),

raw as (
  select
    order_status      as order_status,
    _fivetran_deleted
  from base
),

cleaned as (
  select
    {{ dbt_utils.generate_surrogate_key(['order_status']) }} as order_status,
    lower(order_status)                                  as order_status,
    _fivetran_deleted
  from raw
  where coalesce(_fivetran_deleted, false) = false
  group by order_status, _fivetran_deleted
)

select * from cleaned
