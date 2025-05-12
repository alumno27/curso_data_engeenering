{{ config(
    materialized = 'view',
    tags         = ['base']
) }}

select
  order_id               as order_id_nk,
  user_id                as user_id_nk,
  promo_id               as promo_id_nk,
  address_id             as address_id_nk,
  shipping_service       as shipping_service_raw,
  shipping_cost          as shipping_cost_raw,
  order_cost             as order_cost_raw,
  order_total            as order_total_raw,
  tracking_id            as tracking_id,
  status                 as order_status_raw,
  created_at             as created_at_raw,
  estimated_delivery_at  as estimated_delivery_raw,
  delivered_at           as delivered_at_raw,
  _fivetran_deleted,
  _fivetran_synced       as synced_at_raw
from {{ source('sql_server_dbo','orders') }}
