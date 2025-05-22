{{ config(
    materialized = 'incremental',
    unique_key = ['order_id', 'product_id']
) }}

with max_synced as (
    select coalesce(max(synced_at), '1900-01-01'::timestamp) as max_synced_at
    from {{ this }}
),

filtered_items as (
    select
        order_id,
        product_id,
        quantity,
        price,
        synced_at
    from {{ ref('stg_sql_server_dbo__orders_items') }}
    {% if is_incremental() %}
      where synced_at > (select max_synced_at from max_synced)
    {% endif %}
),

final as (
    select
        order_id,
        product_id,
        quantity,
        price,
        quantity * price as order_item_total,
        synced_at,
        CAST(DATE_TRUNC('day', synced_at) AS DATE) AS synced_at_date
    from filtered_items
)

select * from final
