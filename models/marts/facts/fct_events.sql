{{ config(
    materialized = 'incremental',
    unique_key = 'event_sk'
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
    
    {% if is_incremental() %}
    -- Solo trae nuevos registros al hacer incremental
    where _fivetran_synced > (select max(synced_at_utc) from {{ this }})
    {% endif %}

    and _fivetran_deleted = false
    and user_id != 'unknown'

),

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
        md5(coalesce(cast(e.event_id as text), '_dbt_utils_surrogate_key_null_')) as event_sk,
        e.event_id,
        e.page_url,
        e.event_type,
        e.user_id,
        e.product_id,
        e.session_id,
        e.order_id,
        e.created_at,
        e.synced_at_utc,
        u.user_id as user_id_fk,
        p.product_id as product_id_fk,
        o.order_id as order_id_fk
    from events e
    left join users u     on e.user_id = u.user_id
    left join products p  on e.product_id = p.product_id
    left join orders o    on e.order_id = o.order_id
)

select * from joined
