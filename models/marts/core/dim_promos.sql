{{ config(
    materialized = 'table',
    
) }}

select
    {{ dbt_utils.generate_surrogate_key(['promo_id']) }} as promo_sk,
    promo_id,
    promo_name,
    discount,
    start_date,
    end_date,
    synced_at_utc
from {{ ref('stg_sql_server_dbo__promos') }}
