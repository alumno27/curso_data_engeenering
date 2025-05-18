{{ config(
    materialized = 'table',
    
) }}

select
    {{ dbt_utils.generate_surrogate_key(['order_status']) }} as order_status_sk,
    order_status
from {{ ref('stg_sql_server_dbo__order_status') }}
