{{ config(
    materialized = 'view',
    database     = env_var('DBT_ENVIRONMENTS') ~ '_SILVER_DB',
    schema       = 'SQL_SERVER_DBO',
    tags         = ['staging']
) }}

with raw as (
    select
        order_id            as order_id_nk,
        product_id          as product_id_nk,
        quantity::int       as quantity,
        _fivetran_deleted,
        _fivetran_synced    as synced_at
    from {{ source('sql_server_dbo','order_items') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['order_id_nk', 'product_id_nk']) }} as order_item_sk,
        order_id_nk,
        product_id_nk,
        coalesce(quantity, 0) as quantity,
        convert_timezone('UTC', synced_at)::timestamp_tz as synced_at_utc,
        _fivetran_deleted
    from raw
    where coalesce(_fivetran_deleted, false) = false
)

select * from cleaned
