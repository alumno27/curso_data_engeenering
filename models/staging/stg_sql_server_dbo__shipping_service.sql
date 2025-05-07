{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} as shipping_service_sk,
    shipping_service
from {{ source('sql_server_dbo', 'orders') }}
where shipping_service is not null
group by shipping_service
