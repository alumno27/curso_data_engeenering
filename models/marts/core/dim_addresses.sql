{{ config(
    materialized = 'table',
    
) }}

select
    address_id_sk             as address_sk,
    address_id_nk             as address_id,
    address,
    state,
    country,
    zipcode,
    synced_at_utc
from {{ ref('stg_sql_server_dbo__addresses') }}
