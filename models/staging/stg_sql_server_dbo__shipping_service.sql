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

cleaned as (
  select
    -- surrogate key por servicio de envío
    {{ dbt_utils.generate_surrogate_key(['shipping_service_raw']) }} as shipping_service_sk,
    -- normalizamos a minúsculas y tratamos nulos
    lower(coalesce(shipping_service_raw, 'desconocido')) as shipping_service,
    -- flag de borrado
    _fivetran_deleted
  from base
  where coalesce(_fivetran_deleted, false) = false
)

select distinct 
  shipping_service_sk,
  shipping_service
from cleaned
