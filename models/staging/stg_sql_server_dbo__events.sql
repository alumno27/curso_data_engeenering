{{ config(materialized='view') }}

with source as (

    select *
    from {{ source("sql_server_dbo", "events") }}

    union all

    select
        'unknown' as event_id,
        null as page_url,
        'unknown' as event_type,
        'unknown' as user_id,
        null as product_id,
        null as session_id,
        null as order_id,
        current_timestamp as created_at,
        false as _fivetran_deleted,
        cast(null as timestamp_tz) as _fivetran_synced

)

select *
from source
