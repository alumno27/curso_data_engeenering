{{ config(
    materialized = 'view',
    tags         = ['base']
) }}

select
  product_id       as product_id_nk,
  name             as name_raw,
  price            as price_raw,
  inventory        as inventory_raw,
  _fivetran_deleted,
  _fivetran_synced as synced_at_raw
from {{ source('sql_server_dbo','products') }}
