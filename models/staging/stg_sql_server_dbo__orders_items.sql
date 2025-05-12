{{ 
  config(
    materialized = 'view',
    database     = env_var('DBT_ENVIRONMENTS') ~ '_SILVER_DB',
    schema       = 'SQL_SERVER_DBO',
    tags         = ['staging']
  ) 
}}

with base as (
  select * from {{ ref('base_sql_server_dbo__order_items') }}
),

cleaned as (
  select
    -- surrogate key sobre order_id + product_id
    {{ dbt_utils.generate_surrogate_key(['order_id_nk', 'product_id_nk']) }} as order_item_sk,
    order_id_nk,
    product_id_nk,
    coalesce(quantity_raw, 0) as quantity,
    -- synced timestamp a UTC
    convert_timezone('UTC', synced_at_raw)::timestamp_tz as synced_at_utc,
    _fivetran_deleted
  from base
  where coalesce(_fivetran_deleted, false) = false
)

select * from cleaned
