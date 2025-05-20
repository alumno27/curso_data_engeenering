{{ config(
    materialized = 'view',
    
) }}

with cleaned as (

    select
        "PRODUCT_ID"         as product_id_nk,
        "MONTH"              as month,
        "QUANTITY"::int      as quantity,
        "_FIVETRAN_SYNCED"   as synced_at_raw
    from {{ source('google_sheets', 'budget') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['product_id_nk', 'month']) }} as budget_sk,
    product_id_nk as product_id,
    month,
    quantity,
    convert_timezone('UTC', synced_at_raw)::timestamp_tz as synced_at_utc
from cleaned
