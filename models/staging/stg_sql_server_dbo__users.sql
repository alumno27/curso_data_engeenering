{{ config(
    materialized = 'view',
    
    
) }}

select
    
    user_id,
    lower(first_name)    as first_name,
    lower(last_name)     as last_name,
    lower(email)         as email,
    phone_number,
    total_orders,
    convert_timezone('UTC', synced_at)::timestamp_tz as synced_at_utc,
    _fivetran_deleted
from {{ ref('base_sql_server_dbo__users') }}
where coalesce(_fivetran_deleted, false) = false
