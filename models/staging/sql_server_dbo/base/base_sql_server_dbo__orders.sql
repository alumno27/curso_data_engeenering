{{ config(
    materialized = 'view',
    tags         = ['base']
) }}
with cte as (
select

  order_id               as order_id,
  user_id                as user_id,
  case when promo_id='' 
  then 'desconocido' 
  else promo_id end as promo_id,                 
  address_id             as address_id,
  shipping_service       as shipping_service,
  shipping_cost          as shipping_cost,
  order_cost             as order_cost,
  order_total            as order_total,
  tracking_id            as tracking_id,
  status                 as order_status,
  created_at             as created_at,
  estimated_delivery_at  as estimated_delivery,
  delivered_at           as delivered_at,
  _fivetran_deleted,
  _fivetran_synced       as synced_at

from {{ source('sql_server_dbo','orders') }}
)

select 
order_id,
user_id,
{{ dbt_utils.generate_surrogate_key(["promo_id"]) }} as promo_id,
address_id,
shipping_service,
shipping_cost,
order_cost,
order_total,
tracking_id,
order_status,
created_at,
delivered_at,
_fivetran_deleted,
synced_at

from cte 

