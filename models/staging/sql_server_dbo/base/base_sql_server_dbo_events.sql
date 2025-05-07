{{ config(materialized="view") }}

with cleaned as (

    select
        event_id,
        page_url,
        event_type,
        user_id,
        product_id,
        session_id,
        order_id,
        created_at,
        _fivetran_deleted,
        case
            when try_cast(_fivetran_synced::string as timestamp_tz) is not null
                then cast(_fivetran_synced as timestamp_tz)
            else null
        end as _fivetran_synced
    from {{ source("sql_server_dbo", "events") }}
    where try_cast(_fivetran_synced::string as timestamp_tz) is not null

),

with_fallback as (

    select * from cleaned

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
from with_fallback
