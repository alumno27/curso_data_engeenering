{{ config(materialized = 'table') }}

select
    md5(cast(coalesce(cast(promo_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as promo_sk,
    promo_id,
    discount,
    _fivetran_synced as synced_at_utc
from {{ ref('stg_sql_server_dbo__promos') }}
