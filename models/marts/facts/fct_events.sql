{{ config(
    materialized = 'table'
) }}

with events as (
    select
        event_id,
        page_url,
        event_type,
        user_id,
        product_id,
        session_id,
        order_id,
        created_at,
        _fivetran_synced as synced_at_utc
    from {{ ref('stg_sql_server_dbo__events') }}
    where _fivetran_deleted = false
      and user_id != 'unknown'
),  -- ← ESTA COMA ES CRUCIAL

users as (
    select user_id from {{ ref('dim_users') }}
),

products as (
    select product_id from {{ ref('dim_products') }}
),

orders as (
    select order_id from {{ ref('fct_orders') }}
),

joined as (
    select
        md5(cast(coalesce(cast(e.event_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as event_sk,
        e.event_id,
        e.page_url,
        e.event_type,
        e.user_id,
        e.product_id,
        e.session_id,
        e.order_id,
        e.created_at,
        e.synced_at_utc,

        -- Relaciones (FKs)
        u.user_id       as user_id_fk,
        p.product_id    as product_id_fk,
        o.order_id      as order_id_fk

    from events e
    left join users u     on e.user_id = u.user_id
    left join products p  on e.product_id = p.product_id
    left join orders o    on e.order_id = o.order_id
)

select * from joined
