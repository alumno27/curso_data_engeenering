{{ config(materialized="view") }}

select
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} as event_id_sk,
    event_id,
    page_url,
    event_type,
    user_id,
    product_id,
    session_id,
    order_id,
    convert_timezone('UTC', created_at) as created_at_utc,
    convert_timezone('UTC', _fivetran_synced) as synced_at_utc,
    _fivetran_deleted
from {{ ref('base_sql_server_dbo__events') }}


