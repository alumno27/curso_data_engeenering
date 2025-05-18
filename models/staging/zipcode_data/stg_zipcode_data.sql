{{ 
  config(
    materialized = 'view',
    tags = ['staging']
  )
}}

with raw as (
    select
        "ZIPCODE"                          as zipcode,
        lower("CITY")                     as city,
        try_cast("POPULATION" as int)     as population,
        try_cast(replace("DENSITY", ',', '.') as float) as density
    from {{ source('zipcode_source', 'cities') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['zipcode']) }} as zipcode_sk,
    zipcode,
    city,
    population,
    density
from raw
