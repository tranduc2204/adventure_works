----------- tránh chồng chéo thời gian lên nhau 
-- Version 1: có hiệu lực từ 2024-01-01 đến 2024-06-30
-- Version 2: có hiệu lực từ 2024-06-01 đến 9999-12-3


{{
    config(
        severity = "error" 
    )
}}



WITH ordered_versions AS (
    SELECT 
        product_key,
        dbt_valid_from,
        dbt_valid_to,
        -- Lấy ngày bắt đầu của phiên bản kế tiếp
        LEAD(dbt_valid_from) OVER (
            PARTITION BY product_key
            ORDER BY dbt_valid_from ASC
        ) AS next_valid_from
    FROM {{ ref('dim_products') }}
)

-- Bắt các trường hợp phiên bản sau bắt đầu TRƯỚC KHI phiên bản trước kết thúc
SELECT 
    product_key,
    dbt_valid_from,
    dbt_valid_to,
    next_valid_from
FROM ordered_versions
WHERE dbt_valid_to IS NOT NULL
  AND next_valid_from IS NOT NULL
  AND next_valid_from < dbt_valid_to
