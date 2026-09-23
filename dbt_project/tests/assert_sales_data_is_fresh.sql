{{ config(severity = "error") }}

-- Kiểm tra ngày lớn nhất trong fct_sales có phải là hôm qua hoặc hôm nay không
SELECT 
    MAX(order_date_key) AS latest_order_date
FROM {{ ref('fct_sales') }}
-- Nếu ngày lớn nhất trong bảng mà nhỏ hơn ngày hôm qua -> Báo lỗi!
HAVING MAX(order_date_key) < TO_VARCHAR(DATEADD(day, -1, CURRENT_DATE()), 'YYYYMMDD')::INT