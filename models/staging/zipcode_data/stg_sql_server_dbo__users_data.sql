{{ config(materialized = 'view') }}

with cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['USER_ID', 'BIRTHDATE']) }} as user_sk,
        USER_ID as user_id,
        lower(SEX) as sex,
        try_cast(BIRTHDATE as date) as birthdate
    from {{ source('zipcode_source', 'users_data') }}
)

select * from cleaned
