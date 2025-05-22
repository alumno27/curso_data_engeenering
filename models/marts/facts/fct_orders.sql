{{ config(
    materialized = 'incremental',
    unique_key = 'order_id'
) }}

with max_synced as (
    select coalesce(max(synced_at_utc), '1900-01-01'::timestamp) as max_synced_at
    from {{ this }}
),

filtered_orders as (
    select
        order_id,
        user_id,
        promo_id,
        address_id,
        created_at,
        delivered_at,
        order_cost,
        shipping_cost,
        order_total,
        synced_at_utc
    from {{ ref('stg_sql_server_dbo__orders') }}
    where synced_at_utc > (select max_synced_at from max_synced)
),

status_joined as (
    select
        o.*,
        s.order_status_sk,
        -- ✅ Aquí agregamos la columna necesaria para los tests
        CAST(DATE_TRUNC('day', o.synced_at_utc) AS DATE) AS synced_at_date
    from filtered_orders o
    left join {{ ref('dim_order_status') }} s
      on lower(
          case
            when o.delivered_at is not null then 'delivered'
            when o.delivered_at is null and o.created_at is not null then 'processing'
            else 'unknown'
          end
        ) = s.order_status
)

select * from status_joined

{% if is_incremental() %}
  where synced_at_utc > (select coalesce(max(synced_at_utc), '1900-01-01'::timestamp) from {{ this }})
{% endif %}
