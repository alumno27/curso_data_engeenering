{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

renamed_casted AS (
    SELECT
          generate (promo_id)
        , product_id
        , quantity
        , month
        , _fivetran_synced AS date_load
    FROM src_promos
    )

SELECT * FROM renamed_casted