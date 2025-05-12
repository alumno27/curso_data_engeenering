{{ 
  config(
    materialized = 'view',
    database     = env_var('DBT_ENVIRONMENTS') ~ '_SILVER_DB',
    schema       = 'SQL_SERVER_DBO',
    tags         = ['staging']
  ) 
}}

with base as (
  select * from {{ ref('base_sql_server_dbo__addresses') }}
),

cleaned as (
  select
    -- surrogate key
    {{ dbt_utils.generate_surrogate_key(['address_id_nk']) }}       as address_id_sk,
    -- natural key
    address_id_nk,
    -- normalized text
    lower(address_raw)      as address,
    lower(state_raw)        as state,
    lower(country_raw)      as country,
    zipcode_raw             as zipcode,
    -- synced timestamp to UTC
    convert_timezone('UTC', synced_at_raw)::timestamp_tz            as synced_at_utc,
    -- deletion flag
    _fivetran_deleted
  from base
  where coalesce(_fivetran_deleted, false) = false
)

select * from cleaned

