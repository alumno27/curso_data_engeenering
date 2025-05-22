{{ config(materialized = 'view') }}

with base as (
  select 
    order_id,
    product_id,
    quantity,
    _fivetran_deleted,
    _fivetran_synced
  from {{ ref('base_sql_server_dbo__order_items') }}
  where coalesce(_fivetran_deleted, false) = false
),

products as (
  select
    product_id,
    name,
    price,
    inventory
  from {{ ref('base_sql_server_dbo__products') }}
),

joined as (
  select
    b.order_id,
    b.product_id,
    p.name as product_name,
    p.price,
    p.inventory,
    b.quantity,
    convert_timezone('UTC', b._fivetran_synced)::timestamp_tz as synced_at,
    {{ dbt_utils.generate_surrogate_key(['b.order_id', 'b.product_id']) }} as order_item_id

  from base b
  left join products p
    on b.product_id = p.product_id
)

select * from joined
