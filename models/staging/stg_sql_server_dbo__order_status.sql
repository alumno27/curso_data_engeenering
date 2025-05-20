{{ config(materialized = 'view') }}

with base as (
    select 
        order_id,
        delivered_at,
        created_at,
        _fivetran_deleted
    from {{ ref('base_sql_server_dbo__orders') }}
    where coalesce(_fivetran_deleted, false) = false
),

with_status as (
    select 
        order_id,
        case
            when delivered_at is not null then 'delivered'
            when created_at is not null then 'processing'
            else 'unknown'
        end as order_status
    from base
),

final as (
    select 
        md5(order_status) as order_status_sk,
        lower(order_status) as order_status
    from with_status
    group by order_status
)

select * from final
