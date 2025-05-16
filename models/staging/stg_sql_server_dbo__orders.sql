{{ config(
    materialized = 'view',
    tags         = ['staging']
) }}

with base as (
    select * from {{ ref('base_sql_server_dbo__orders') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} as promo_id,
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} as address_id,

        order_id,
        user_id,
        shipping_service,
        shipping_cost,
        order_cost,
        order_total,
        tracking_id,
        order_status,
        created_at,
        estimated_delivery,
        delivered_at,
        synced_at,
        _fivetran_deleted
        
        
    from base
    where coalesce(_fivetran_deleted, false) = false
)

select * from enriched
