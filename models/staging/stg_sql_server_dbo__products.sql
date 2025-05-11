{{ 
  config(
    materialized = 'view',
    database     = env_var('DBT_ENVIRONMENTS') ~ '_SILVER_DB',
    schema       = 'SQL_SERVER_DBO',
    tags         = ['staging']
  ) 
}}

with raw as (
  select
    product_id            as product_id_nk,
    name                  as name_raw,
    price::numeric        as price,
    inventory::int        as inventory,
    _fivetran_deleted,
    _fivetran_synced      as synced_at
  from {{ source('sql_server_dbo', 'products') }}
),

cleaned as (
  select
    -- surrogate key
    {{ dbt_utils.generate_surrogate_key(['product_id_nk']) }}  as product_id_sk,
    -- natural key
    product_id_nk,
    -- normalized fields
    lower(name_raw)      as name,
    coalesce(price, 0)   as price,
    coalesce(inventory, 0) as inventory,
    -- sync timestamp → UTC
    convert_timezone('UTC', synced_at)::timestamp_tz as synced_at_utc,
    -- deletion flag
    _fivetran_deleted
  from raw
  where coalesce(_fivetran_deleted, false) = false
)

select * from cleaned
