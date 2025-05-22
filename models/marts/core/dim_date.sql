{{ config(
    materialized = 'table',
    tags = ['dimension']
) }}

-- 📆 Generar fechas entre 2000 y 2050 (~55 años)
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2000-01-01') AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 20000))
),

enriched AS (
    SELECT
        date,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(DAY FROM date) AS day,
        EXTRACT(QUARTER FROM date) AS quarter,
        EXTRACT(WEEK FROM date) AS week,
        TO_CHAR(date, 'Day') AS day_name,
        TO_CHAR(date, 'Month') AS month_name,
        CASE WHEN EXTRACT(DOW FROM date) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM date_spine
)

SELECT * FROM enriched
