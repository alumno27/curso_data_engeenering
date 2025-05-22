select
    PRODUCT_ID,
    CATEGORY,
    TAGS,
    SUPPLIER_ID,
    try_cast(replace(AVERAGE_RATING, ',', '.') as float) as average_rating
from {{ source('products_enriched', 'PRODUCTS_ANALYTICS') }}
