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
    product_id_nk     as product_id,
    name_raw          as name,
    price_raw         as price,
    inventory_raw     as inventory,
    synced_at_raw     as synced_at_utc,
    _fivetran_deleted
  from base
  where coalesce(_fivetran_deleted, false) = false
)

select * 
from cleaned
