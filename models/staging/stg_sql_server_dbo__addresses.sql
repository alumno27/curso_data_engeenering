with base as (
  select * from ALUMNO27_DEV_SILVER_DB.dbt_rricardo_sql_server_dbo.base_sql_server_dbo__addresses
),

cleaned as (
  select
    address_id,  -- clave natural
    lower(address) as address,
    lower(state) as state,
    lower(country) as country,
    zipcode as zipcode,
    convert_timezone('UTC', synced_at)::timestamp_tz as synced_at_utc,
    _fivetran_deleted
  from base
  where coalesce(_fivetran_deleted, false) = false
)

select * from cleaned
