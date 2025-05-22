{{ config(materialized = 'table') }}

with base_products as (
    select * from {{ ref('stg_sql_server_dbo__products') }}
),

enriched as (
    select * from {{ ref('stg_products_enriched') }}
),

final as (
    select
        p.product_id,
        p.name,
        p.price,
        p.inventory,
        p.synced_at,
        e.category,
        e.tags,
        e.supplier_id,
        e.average_rating
    from base_products p
    left join enriched e on p.product_id = e.product_id
)

select * from final











