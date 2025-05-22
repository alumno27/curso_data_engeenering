{{ config(materialized = 'table') }}

-- Modelo de dimensión de usuarios, combinando datos personales y de actividad
-- Integra información desde stg_users y stg_users_data
-- Excluye registros sin fecha de nacimiento

with base as (
    select * from {{ ref('stg_sql_server_dbo__users') }}
),

extra as (
    select * from {{ ref('stg_sql_server_dbo__users_data') }}
),

-- Eliminar duplicados, tomando la fila más reciente por usuario en base
dedup_base as (
    select *
    from (
        select *,
            row_number() over (partition by user_id order by synced_at_utc desc) as rn
        from base
    )
    where rn = 1
),

-- Eliminar duplicados en extra (no hay columna de tiempo, se usa user_id)
dedup_extra as (
    select *
    from (
        select *,
            row_number() over (partition by user_id order by user_id) as rn
        from extra
    )
    where rn = 1
),

-- Unión entre usuarios y atributos adicionales (sexo, nacimiento)
joined as (
    select
        b.user_id,
        b.first_name,
        b.last_name,
        b.email,
        b.phone_number,
        b.total_orders,
        e.sex,
        try_cast(e.birthdate as date) as birthdate,
        b.synced_at_utc
    from dedup_base b
    left join dedup_extra e on b.user_id = e.user_id
),

-- Solo conservar usuarios con fecha de nacimiento válida
final as (
    select *
    from joined
    where birthdate is not null
)

select * from final
