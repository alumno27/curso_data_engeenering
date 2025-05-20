{{ config(materialized = "view") }}

with cleaned as (

    select
        "EVENT_ID"            as event_id,
        "PAGE_URL"            as page_url,
        "EVENT_TYPE"          as event_type,
        "USER_ID"             as user_id,
        "PRODUCT_ID"          as product_id,
        "SESSION_ID"          as session_id,
        "ORDER_ID"            as order_id,
        "CREATED_AT"          as created_at,
        "_FIVETRAN_DELETED"   as _fivetran_deleted,
        case
            when try_cast("_FIVETRAN_SYNCED"::string as timestamp_tz) is not null
                then cast("_FIVETRAN_SYNCED" as timestamp_tz)
            else null
        end as _fivetran_synced
    from {{ source("sql_server_dbo", "events") }}
    where try_cast("_FIVETRAN_SYNCED"::string as timestamp_tz) is not null

),

with_fallback as (
    select * from cleaned

    union all

    select
        'unknown' as event_id,
        null      as page_url,
        'unknown' as event_type,
        'unknown' as user_id,
        null      as product_id,
        null      as session_id,
        null      as order_id,
        current_timestamp as created_at,
        false     as _fivetran_deleted,
        cast(null as timestamp_tz) as _fivetran_synced
)

select *
from with_fallback
