{{ config(materialized = 'table') }}

select 
    order_status_sk,
    order_status
from {{ ref('stg_sql_server_dbo__order_status') }}
