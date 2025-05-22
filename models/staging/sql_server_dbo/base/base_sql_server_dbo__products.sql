{{ config(
    materialized = 'view',
    tags = ['base']
) }}

select
  "PRODUCT_ID"       as product_id,
  "NAME"             as name,
  "PRICE"            as price,
  "INVENTORY"        as inventory,
  "_FIVETRAN_DELETED" as _fivetran_deleted,
  "_FIVETRAN_SYNCED"  as synced_at
from {{ source('sql_server_dbo','products') }}
