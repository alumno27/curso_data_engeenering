{{ 
  config(
    materialized = 'view',
    
  ) 
}}

with raw as (
  select
    order_id                 as order_id_nk,
    user_id                  as user_id_nk,
    promo_id                 as promo_id_nk,
    address_id               as address_id_nk,
    shipping_service         as shipping_service_raw,
    shipping_cost::numeric   as shipping_cost,
    order_cost::numeric      as order_subtotal,
    order_total::numeric     as order_total,
    tracking_id,
    status                   as order_status,
    created_at,
    estimated_delivery_at    as estimated_delivery_at_raw,
    delivered_at             as delivered_at_raw,
    _fivetran_deleted,
    _fivetran_synced         as synced_at
  from {{ source('sql_server_dbo','orders') }}
),

cleaned as (
  select
    -- surrogate key para pedido
    {{ dbt_utils.generate_surrogate_key(['order_id_nk']) }} as order_id_sk,

    -- llaves naturales
    order_id_nk,
    user_id_nk,
    promo_id_nk,
    address_id_nk,

    -- surrogate key para servicio de envío (fallback 'desconocido')
    {{ dbt_utils.generate_surrogate_key(["coalesce(shipping_service_raw, 'desconocido')"]) }} 
      as shipping_service_sk,

    -- costes, evitando NULLs
    coalesce(shipping_cost, 0)  as shipping_cost,
    coalesce(order_subtotal, 0) as order_subtotal,
    coalesce(order_total, 0)    as order_total,

    tracking_id,
    order_status,

    -- fechas convertidas a UTC
    convert_timezone('UTC', created_at)::timestamp_tz                       as created_at_utc,
    convert_timezone('UTC', estimated_delivery_at_raw)::timestamp_tz       as estimated_delivery_utc,
    convert_timezone('UTC', delivered_at_raw)::timestamp_tz                as delivered_at_utc,
    convert_timezone('UTC', synced_at)::timestamp_tz                       as synced_at_utc,

    -- flag eliminación
    _fivetran_deleted
  from raw
  where coalesce(_fivetran_deleted, false) = false
)

select * from cleaned
