{{ config(
    materialized = 'view',
    
) }}

select
  order_id       as order_id,
  product_id     as product_id,
  quantity       as quantity,
  _fivetran_deleted,
  _fivetran_synced as synced_at
from {{ source('sql_server_dbo','order_items') }}
