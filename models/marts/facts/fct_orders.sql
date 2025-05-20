{{ config(
    materialized = 'table'
) }}

with orders as (
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
),

status_joined as (
  select
    o.*,
    s.order_status_sk
  from orders o
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
