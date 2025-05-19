{{ config(
    materialized = 'view',
    
) }}

select
  order_id ,    
  product_id ,    
  quantity   ,    
  _fivetran_deleted,
  _fivetran_synced 
from {{ source('sql_server_dbo','order_items') }}
