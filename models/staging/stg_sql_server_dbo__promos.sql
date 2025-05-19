{{ 
  config(
    materialized = 'view'
  ) 
}}

with base as (
  select * from {{ ref('base_sql_server_dbo__promos') }}
),

cleaned as (
  select
    {{ dbt_utils.generate_surrogate_key(['promo_id']) }} as promo_id_sk,
    promo_id,
    lower(promo_id) as promo_description,
    coalesce(discount, 0) as discount,
    status,
    _FIVETRAN_SYNCED 
    
  from base
  
)

select * from cleaned
