
{{ config(materialized="view") }}


with raw as (
  select
    address_id           as address_id_nk,
    address              as address_raw,
    state                as state_raw,
    country              as country_raw,
    zipcode::varchar     as zipcode,
    _fivetran_deleted,
    _fivetran_synced     as synced_at
  from {{ source('sql_server_dbo','addresses') }}
)

, cleaned as (
  select
    {{ dbt_utils.generate_surrogate_key(['address_id_nk']) }} as address_id_sk,
    address_id_nk,
    lower(address_raw)    as address,
    lower(state_raw)      as state,
    lower(country_raw)    as country,
    zipcode,
    convert_timezone('UTC', synced_at)::timestamp_tz as synced_at_utc,
    _fivetran_deleted
  from raw
  -- Tratar NULL como FALSE para no filtrar registros válidos
  where coalesce(_fivetran_deleted, false) = false
)

select * from cleaned
