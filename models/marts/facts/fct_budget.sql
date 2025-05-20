{{ config(
    materialized = 'table'
) }}

select
    budget_sk,
    product_id,
    month,
    quantity,
    synced_at_utc
from {{ ref('stg_google_sheets__budget') }}
