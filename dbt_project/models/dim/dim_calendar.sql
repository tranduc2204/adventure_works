-- models/dim/dim_calendar.sql
{{ config(
    materialized = 'table'
) }}

WITH source_calendar AS (
    SELECT 
        cast(date as date) as full_date
    FROM {{ ref('src_calendar') }}
)

SELECT
    -- 1. Date Surrogate Key
    TO_VARCHAR(full_date, 'YYYYMMDD')::INT AS date_key,
    
    -- 2. Ngày chuẩn
    full_date,
    
    -- 3. Phân cấp Năm / Quý / Tháng / Ngày
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    CONCAT('Q', EXTRACT(QUARTER FROM full_date), '-', EXTRACT(YEAR FROM full_date)) AS quarter_name,
    
    EXTRACT(MONTH FROM full_date) AS month,
    MONTHNAME(full_date) AS month_name,
    TO_VARCHAR(full_date, 'YYYY-MM') AS year_month,
    
    EXTRACT(DAY FROM full_date) AS day_of_month,
    EXTRACT(WEEK FROM full_date) AS week_of_year,
    
    -- 4. Thứ trong tuần
    DAYNAME(full_date) AS day_name,
    DAYOFWEEK(full_date) AS day_of_week,
    
    -- 5. Cờ đánh dấu ngày cuối tuần (Thứ 7 = 6, CN = 0/7 tùy config)
    CASE 
        WHEN DAYNAME(full_date) IN ('Sat', 'Sun') THEN TRUE 
        ELSE FALSE 
    END AS is_weekend

FROM source_calendar
ORDER BY full_date