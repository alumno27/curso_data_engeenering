{{ config(materialized="view") }}

select
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_id_sk,
    order_id,
    user_id,
    promo_id,
    address_id,
    {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} as shipping_service_sk,
    shipping_cost,
    order_cost,
    order_total,
    tracking_id,
    status,
    convert_timezone('UTC', created_at) as created_at_utc,
    convert_timezone('UTC', estimated_delivery_at) as estimated_delivery_utc,
    convert_timezone('UTC', delivered_at) as delivered_at_utc,
    convert_timezone('UTC', _fivetran_synced) as synced_at_utc,
    _fivetran_deleted
from {{ source('sql_server_dbo', 'orders') }}
