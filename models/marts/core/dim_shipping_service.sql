{{ config(
    materialized = 'table'
) }}

with source_data as (

    select distinct
        shipping_service
    from {{ ref('stg_sql_server_dbo__orders') }}
    where shipping_service is not null

),

final as (

    select
        md5(cast(coalesce(cast(shipping_service as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as shipping_service_sk,
        shipping_service
    from source_data

)

select * from final
