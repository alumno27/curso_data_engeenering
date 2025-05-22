{{ config(materialized='table') }}

with base as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['product_id', 'month']) }} as budget_sk
    from {{ ref('stg_google_sheets__budget') }}
)

select
    budget_id,
    product_id,
    month,
    quantity,
    synced_at
from base
