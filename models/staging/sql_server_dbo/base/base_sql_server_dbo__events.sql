{{ config(materialized = 'view') }}

select
  "EVENT_ID"           as event_id,
  "PAGE_URL"           as page_url,
  "EVENT_TYPE"         as event_type,
  "USER_ID"            as user_id,
  "PRODUCT_ID"         as product_id,
  "SESSION_ID"         as session_id,
  "ORDER_ID"           as order_id,
  "CREATED_AT"         as created_at,
  "_FIVETRAN_DELETED"  as _fivetran_deleted,
  "_FIVETRAN_SYNCED"   as _fivetran_synced
from {{ source("sql_server_dbo", "events") }}
