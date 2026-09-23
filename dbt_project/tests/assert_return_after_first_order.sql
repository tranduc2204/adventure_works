{{
    config(
        severity = "warn",
        warn_if ='>0',
        error_if = '>50'
    )
}}


WITH first_sale AS (
    SELECT 
        product_key,
        MIN(order_date_key) AS first_order_date_key
    FROM {{ ref('fct_sales') }}
    GROUP BY product_key
),

first_return AS (
    SELECT 
        product_key,
        MIN(return_date_key) AS first_return_date_key
    FROM {{ ref('fct_returns') }}
    GROUP BY product_key
)

-- Tìm sản phẩm có ngày trả hàng đầu tiên trước cả ngày bán đầu tiên
---- Find products with a first return date earlier than the first sale date
SELECT 
    r.product_key,
    r.first_return_date_key,
    s.first_order_date_key
FROM first_return r
JOIN first_sale s 
  ON r.product_key = s.product_key
WHERE r.first_return_date_key < s.first_order_date_key