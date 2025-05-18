{{ config(
    materialized = 'table',
    
) }}

select
    {{ dbt_utils.generate_surrogate_key(['address_id']) }} as address_sk,
    address_id,
    address,
    state,
    country,
    zipcode,
    synced_at_utc
from {{ ref('stg_sql_server_dbo__addresses') }}
