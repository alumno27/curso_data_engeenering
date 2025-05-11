{{ config(
    materialized = 'view',
    database     = env_var('DBT_ENVIRONMENTS') ~ '_SILVER_DB',
    schema       = 'SQL_SERVER_DBO',
    tags         = ['staging']
) }}

with source as (

  -- Desglose explícito de columnas, en el orden correcto
  select
    event_id,
    page_url,
    event_type,
    user_id,
    product_id,
    session_id,
    order_id,
    created_at,
    _fivetran_deleted,
    _fivetran_synced
  from {{ source('sql_server_dbo','events') }}

  union all

  -- Mismo orden y tipos idénticos
  select
    'unknown'                     as event_id,           -- varchar
    null                          as page_url,           -- varchar
    'unknown'                     as event_type,         -- varchar
    'unknown'                     as user_id,            -- varchar
    null                          as product_id,         -- varchar
    null                          as session_id,         -- varchar
    null                          as order_id,           -- varchar
    current_timestamp()           as created_at,         -- timestamp_tz
    false                         as _fivetran_deleted,  -- boolean
    null::timestamp_tz            as _fivetran_synced    -- timestamp_tz
)

select * from source
