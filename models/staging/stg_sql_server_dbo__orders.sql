{{ config(materialized="view") }}

-- Modelo staging para pedidos (orders)
-- Normaliza nombres de columnas, genera claves surrogate, convierte fechas a UTC
-- y maneja valores nulos como "desconocido" cuando es necesario.

select
    -- Clave surrogate para el pedido
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_id_sk,

    -- IDs originales del sistema transaccional
    order_id as order_id_nk,
    user_id as user_id_nk,
    promo_id as promo_id_nk,
    address_id as address_id_nk,

    -- Clave surrogate del servicio de envío (valor nulo reemplazado por 'desconocido')
    {{ dbt_utils.generate_surrogate_key(["coalesce(shipping_service, 'desconocido')"]) }} as shipping_service_sk,

    -- Costes asociados al pedido
    shipping_cost,
    order_cost as order_subtotal,
    order_total as order_total,  -- total = subtotal + shipping

    -- Información logística
    tracking_id,
    status as order_status,

    -- Fechas convertidas a UTC 
    convert_timezone('UTC', created_at) as created_at_utc,
    convert_timezone('UTC', estimated_delivery_at) as estimated_delivery_utc,
    convert_timezone('UTC', delivered_at) as delivered_at_utc,
    convert_timezone('UTC', _fivetran_synced) as synced_at_utc,

    -- Indicador de eliminación en origen (debe filtrar en downstream si es true)
    _fivetran_deleted

from {{ source('sql_server_dbo', 'orders') }}

-- Filtra registros marcados como eliminados
where _fivetran_deleted is null or _fivetran_deleted = false
