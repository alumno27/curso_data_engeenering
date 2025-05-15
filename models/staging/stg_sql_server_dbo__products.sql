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
    product_id_nk       as product_id_nk,
    name_raw            as name_raw,
    price_raw           as price_raw,
    inventory_raw        as inventory_raw,
    _fivetran_deleted,
    synced_at_raw as synced_at_raw
  from base
  where coalesce(_fivetran_deleted, false) = false
)

select * 
from cleaned
