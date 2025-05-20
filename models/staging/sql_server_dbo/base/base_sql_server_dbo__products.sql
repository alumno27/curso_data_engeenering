{{ config(
    materialized = 'view',
    tags = ['base']
) }}

select
  "PRODUCT_ID"       as product_id_nk,
  "NAME"             as name_raw,
  "PRICE"            as price_raw,
  "INVENTORY"        as inventory_raw,
  "_FIVETRAN_DELETED" as _fivetran_deleted,
  "_FIVETRAN_SYNCED"  as synced_at_raw
from {{ source('sql_server_dbo','products') }}
