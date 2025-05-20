{{ config(
    materialized = 'table'
) }}

with order_items as (
    select
        order_id,
        product_id,
        quantity,
        synced_at_utc
    from {{ ref('stg_sql_server_dbo__orders_items') }}
),

product_details as (
    select
        product_id,
        price
    from {{ ref('dim_products') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['oi.order_id', 'oi.product_id']) }} as order_item_sk,
        oi.order_id,
        oi.product_id,
        dp.price,
        oi.quantity,
        oi.quantity * dp.price as order_item_total,
        oi.synced_at_utc
    from order_items oi
    left join product_details dp
        on oi.product_id = dp.product_id
)

select * from joined
