{{ config(
    materialized = 'view',
    tags         = ['base']
) }}

select
  address_id       as address_id_nk,
  address          as address_raw,
  state            as state_raw,
  country          as country_raw,
  zipcode          as zipcode_raw,
  _fivetran_deleted,
  _fivetran_synced as synced_at_raw
from {{ source('sql_server_dbo','addresses') }}

