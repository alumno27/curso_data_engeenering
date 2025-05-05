{{ config(materialized="view") }}

with
    src_orders as (
        select *
        from {{ source("sql_server_dbo", "orders") }}

        union all

        select
            'desconocido' as promo_id,
            0 as discount,
            'inactive' as status,
            null as _fivetran_deleted,
            null as _fivetran_synced

    ),

    renamed_casted as (
        select
            {{ dbt_utils.generate_surrogate_key(["promo_id"]) }} as promo_id,
            promo_id as descripcion,
            discount,
            status,
            _fivetran_synced as date_load
        from src_promos
    )

select *
from renamed_casted
