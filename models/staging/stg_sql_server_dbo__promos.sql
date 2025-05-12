{{ 
  config(
    materialized = 'view',
    database     = env_var('DBT_ENVIRONMENTS') ~ '_SILVER_DB',
    schema       = 'SQL_SERVER_DBO',
    tags         = ['staging']
  ) 
}}

with base as (
  select * from {{ ref('base_sql_server_dbo__promos') }}
),

cleaned as (
  select
    -- surrogate key para promo
    {{ dbt_utils.generate_surrogate_key(['promo_id_nk']) }}    as promo_id_sk,

    -- llave natural
    promo_id_nk,

    -- descripción (normalizada)
    lower(promo_name_raw)          as promo_description,

    -- descuento en formato numérico
    coalesce(discount_raw, 0)      as discount,

    -- fechas de vigencia
    start_date_raw                 as start_date,
    end_date_raw                   as end_date,

    -- sync timestamp en UTC
    convert_timezone('UTC', synced_at_raw)::timestamp_tz   as synced_at_utc,

    -- flag de borrado
    _fivetran_deleted
  from base
  where coalesce(_fivetran_deleted, false) = false
)

select * from cleaned


