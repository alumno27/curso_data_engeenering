{{ 
  config(
    materialized = 'view',
    database     = env_var('DBT_ENVIRONMENTS') ~ '_SILVER_DB',
    schema       = 'SQL_SERVER_DBO',
    tags         = ['staging']
  )
}}

with base as (
  select * from {{ ref('base_sql_server_dbo__orders') }}
),

cleaned as (
  select
    -- surrogate key para el pedido
    {{ dbt_utils.generate_surrogate_key(['order_id_nk']) }} as order_id_sk,

    -- llaves naturales
    order_id_nk,
    user_id_nk,
    promo_id_nk,
    address_id_nk,

    -- surrogate key para servicio de envío (fallback 'desconocido')
    {{ dbt_utils.generate_surrogate_key([
         "coalesce(shipping_service_raw, 'desconocido')"
       ]) }} as shipping_service_sk,

    -- costes, evitando NULLs
    coalesce(shipping_cost_raw, 0)  as shipping_cost,
    coalesce(order_cost_raw, 0)     as order_subtotal,
    coalesce(order_total_raw, 0)    as order_total,

    tracking_id,

    -- estado del pedido
    lower(order_status_raw)         as order_status,

    -- fechas convertidas a UTC
    convert_timezone('UTC', created_at_raw)::timestamp_tz           as created_at_utc,
    convert_timezone('UTC', estimated_delivery_raw)::timestamp_tz   as estimated_delivery_utc,
    convert_timezone('UTC', delivered_at_raw)::timestamp_tz        as delivered_at_utc,
    convert_timezone('UTC', synced_at_raw)::timestamp_tz           as synced_at_utc,

    _fivetran_deleted
  from base
  where coalesce(_fivetran_deleted, false) = false
)

select * from cleaned
