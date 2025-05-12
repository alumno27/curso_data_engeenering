{{ config(
    materialized = 'view',
    tags         = ['base']
) }}

select
  user_id          as user_id_nk,
  first_name       as first_name_raw,
  last_name        as last_name_raw,
  email            as email_raw,
  phone_number     as phone_number_raw,
  total_orders     as total_orders_raw,
  _fivetran_deleted,
  _fivetran_synced as synced_at_raw
from {{ source('sql_server_dbo','users') }}
