{{ 
  config(
    materialized = 'view',
    
    tags         = ['staging']
  ) 
}}

with base as (
  select * from {{ ref('base_sql_server_dbo__orders') }}
),

cleaned as (
  select
    -- surrogate key por servicio de envío
    {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} as shipping_service_id,
    -- normalizamos a minúsculas y tratamos nulos
    lower(coalesce(shipping_service, 'desconocido')) as shipping_service,
    -- flag de borrado
    _fivetran_deleted
  from base
  where coalesce(_fivetran_deleted, false) = false
)

select distinct 
  shipping_service_id,
  shipping_service
from cleaned
