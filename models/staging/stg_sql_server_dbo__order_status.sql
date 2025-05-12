{{ 
  config(
    materialized = 'view',
    database     = env_var('DBT_ENVIRONMENTS') ~ '_SILVER_DB',
    schema       = 'SQL_SERVER_DBO',
    tags         = ['staging']
  ) 
}}

with base as (
  select * from {{ ref('base_sql_server_dbo__orders') }}
),

raw as (
  select
    order_status_raw      as order_status_nk,
    _fivetran_deleted
  from base
),

cleaned as (
  select
    {{ dbt_utils.generate_surrogate_key(['order_status_nk']) }} as order_status_sk,
    lower(order_status_nk)                                  as order_status,
    _fivetran_deleted
  from raw
  where coalesce(_fivetran_deleted, false) = false
  group by order_status_nk, _fivetran_deleted
)

select * from cleaned
