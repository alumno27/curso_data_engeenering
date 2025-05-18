{{ config(
    materialized = 'view',
    tags         = ['base']
) }}

select
  "USER_ID"          as user_id,
  "FIRST_NAME"       as first_name,
  "LAST_NAME"        as last_name,
  "EMAIL"            as email,
  "PHONE_NUMBER"     as phone_number,
  "TOTAL_ORDERS"     as total_orders,
  _fivetran_deleted,
  _fivetran_synced   as synced_at
from {{ source('sql_server_dbo','users') }}

