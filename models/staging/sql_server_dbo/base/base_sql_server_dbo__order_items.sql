{{ config(materialized = 'view') }}

select
  "ORDER_ID"       as order_id,
  "PRODUCT_ID"     as product_id,
  "QUANTITY"       as quantity,
  "_FIVETRAN_DELETED" as _fivetran_deleted,
  "_FIVETRAN_SYNCED"  as _fivetran_synced
from {{ source('sql_server_dbo', 'order_items') }}
