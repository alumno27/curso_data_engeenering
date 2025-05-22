{{ config(materialized = 'view') }}

with base as (
    select * from {{ ref('base_products_enriched') }}
),

cleaned as (
    select
        PRODUCT_ID       as product_id,
        CATEGORY         as category,
        TAGS             as tags,
        SUPPLIER_ID      as supplier_id,
        try_cast(AVERAGE_RATING as float) as average_rating
    from base
)

select * from cleaned
