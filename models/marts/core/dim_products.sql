{{ config(materialized = 'table') }}

select
    md5(cast(coalesce(cast(product_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as product_sk,
    product_id,
    name,
    price,
    inventory,
    synced_at_utc
from {{ ref('stg_sql_server_dbo__products') }}
