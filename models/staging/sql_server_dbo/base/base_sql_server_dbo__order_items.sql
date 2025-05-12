{{ config(
    materialized = 'view',
    tags         = ['base']
) }}

select
  order_id       as order_id_nk,
  product_id     as product_id_nk,
  quantity       as quantity_raw,
  _fivetran_deleted,
  _fivetran_synced as synced_at_raw
from {{ source('sql_server_dbo','order_items') }}
