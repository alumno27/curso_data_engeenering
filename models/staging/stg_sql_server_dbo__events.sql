{{ config(
    materialized = 'view',
    tags         = ['base']
) }}

select
  event_id                   as event_id_nk,
  page_url                   as page_url_raw,
  event_type                 as event_type_raw,
  user_id                    as user_id_nk,
  product_id                 as product_id_nk,
  session_id                 as session_id_raw,
  order_id                   as order_id_nk,
  created_at                 as created_at_raw,
  _fivetran_deleted          as _fivetran_deleted,
  _fivetran_synced           as synced_at_raw
from {{ source('sql_server_dbo','events') }}



