{{ 
  config(
    materialized = 'view',
  ) 
}}

-- Obtenemos los datos base del modelo base (renombrado y tipado mínimo)
with base as (
  select * 
  from {{ ref('base_sql_server_dbo__order_items') }}
),

-- Traemos el precio del producto desde el staging de productos para enriquecer
products as (
  select 
    product_id, 
    price
  from {{ ref('stg_sql_server_dbo__products') }}
),

-- Combinamos los ítems del pedido con los productos para incorporar el precio
joined as (
  select
    b.order_id,
    b.product_id,
    coalesce(b.quantity, 0) as quantity,  -- aseguramos que no haya NULLs
    p.price,                             -- precio actual del producto (no historificado)
    b.synced_at,
    b._fivetran_deleted
  from base b
  left join products p
    on b.product_id = p.product_id
),

-- Generamos surrogate key y normalizamos campos
final as (
  select
    -- surrogate key compuesta: cada combinación order_id + product_id es única
    {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} as order_item_sk,
    order_id,
    product_id,
    quantity,
    price,
    convert_timezone('UTC', synced_at)::timestamp_tz as synced_at_utc,
    _fivetran_deleted
  from joined
  where coalesce(_fivetran_deleted, false) = false  -- filtramos registros eliminados
)

-- Resultado final limpio para usar en capa de mart
select * from final
