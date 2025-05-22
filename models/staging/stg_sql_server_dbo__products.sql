{{ 
  config(
    materialized = 'view'
  ) 
}}

with base as (
  select * 
  from {{ ref('base_sql_server_dbo__products') }}
),

cleaned as (
  select
    product_id    as product_id,
    name          as name,
    price        as price,
    inventory     as inventory,
    synced_at    as synced_at,
    _fivetran_deleted
  from base
  where coalesce(_fivetran_deleted, false) = false
)

select * 
from cleaned
