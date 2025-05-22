{{ config(
    materialized = 'view',
    tags         = ['base']
) }}

select
  address_id       as address_id,
  address          as address,
  state            as state,
  country          as country,
  zipcode          as zipcode,
  _fivetran_deleted,
  _fivetran_synced as synced_at
from {{ source('sql_server_dbo','addresses') }}

