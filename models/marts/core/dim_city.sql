{{ config(
    materialized = 'table',
   
) }}





with raw as (
    select
        zipcode,
        lower(city) as city,
        try_cast(replace(cast(population as string), ',', '.') as float) as population,
        try_cast(replace(cast(density as string), ',', '.') as float) as density
    from {{ ref('stg_zipcode_data') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['zipcode']) }} as zipcode_sk,
    *
from raw
