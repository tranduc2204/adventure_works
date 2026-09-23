---- So sánh với trung bình 7 ngày qua  
---- so với trung bình 7 ngày qua data có đều không nếu không đều raise error 


-- tests/assert_sales_volume_anomaly.sql
{{ config(severity = "error") }}

WITH daily_counts AS (
    SELECT 
        order_date_key,
        COUNT(*) AS total_orders
    FROM {{ ref('fct_sales') }}
    -- Lấy dữ liệu 8 ngày gần nhất
    WHERE order_date_key >= TO_VARCHAR(DATEADD(day, -8, CURRENT_DATE()), 'YYYYMMDD')::INT
    GROUP BY order_date_key
),

stats AS (
    SELECT 
        -- Số đơn ngày hôm qua
        MAX(CASE WHEN order_date_key = TO_VARCHAR(DATEADD(day, -1, CURRENT_DATE()), 'YYYYMMDD')::INT THEN total_orders END) AS yesterday_orders,
        -- Trung bình số đơn của 7 ngày trước đó
        AVG(CASE WHEN order_date_key < TO_VARCHAR(DATEADD(day, -1, CURRENT_DATE()), 'YYYYMMDD')::INT THEN total_orders END) AS avg_7d_orders
    FROM daily_counts
)

-- PHÁT HIỆN BẤT THƯỜNG:
SELECT 
    yesterday_orders,
    avg_7d_orders,
    (yesterday_orders / NULLIF(avg_7d_orders, 0)) * 100 AS percentage_of_normal
FROM stats
-- Nếu số đơn hôm qua sụt giảm dưới 30% so với mức trung bình bình thường -> BÁO ĐỘNG ĐỎ!
WHERE yesterday_orders < (avg_7d_orders * 0.3)