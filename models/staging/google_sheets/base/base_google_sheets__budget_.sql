{{ config(
    materialized = 'view',
    tags = ['base']
) }}

select
    -- Clave surrogate (hasheada) para presupuesto por producto y mes
    {{ dbt_utils.generate_surrogate_key(['"PRODUCT_ID"', '"MONTH"']) }} as budget,
    
    -- Campos originales
    "PRODUCT_ID"       as product_id,
    "MONTH"            as month,
    "QUANTITY"::int    as quantity,
    "_FIVETRAN_SYNCED" as synced_at
from {{ source('google_sheets', 'budget') }}
