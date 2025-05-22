{{ config(
    materialized = 'view',
    tags = ['base']
) }}

with base as (
  select * from {{ source('sql_server_dbo', 'orders') }}
),

cleaned as (
  select
    ORDER_ID as order_id,
    USER_ID as user_id,
    case when PROMO_ID = '' then 'desconocido' else PROMO_ID end as promo_id,
    ADDRESS_ID as address_id,
    SHIPPING_SERVICE as shipping_service,
    SHIPPING_COST as shipping_cost,
    ORDER_COST as order_cost,
    ORDER_TOTAL as order_total,
    TRACKING_ID as tracking_id,
    STATUS as order_status,
    CREATED_AT as created_at,
    ESTIMATED_DELIVERY_AT as estimated_delivery,
    DELIVERED_AT as delivered_at,
    convert_timezone('UTC', _FIVETRAN_SYNCED)::timestamp_tz as synced_at_utc,
    _FIVETRAN_DELETED as fivetran_deleted
  from base
  where coalesce(_FIVETRAN_DELETED, false) = false
)

select * from cleaned
