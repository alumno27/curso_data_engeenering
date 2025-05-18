{{ config(
    materialized = 'table',
   
) }}

select
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,
    user_id,
    first_name,
    last_name,
    email,
    phone_number,
    total_orders,
    synced_at_utc
from {{ ref('stg_sql_server_dbo__users') }}
