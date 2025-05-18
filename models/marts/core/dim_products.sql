{{ config(
    materialized = 'table',
   
) }}

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
    product_id,
    name,
    price,
    inventory,
    synced_at_utc
from {{ ref('stg_sql_server_dbo__products') }}
