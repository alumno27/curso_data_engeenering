{{ config(materialized="view") }}

select
    {{ dbt_utils.generate_surrogate_key(['promo_id']) }} as promo_id_sk,
    promo_id,
    descripcion as promo_description,
    discount,
    status,
    convert_timezone('UTC', date_load) as date_load_utc
from {{ ref('base_sql_server_dbo__promos') }}
