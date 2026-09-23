{{ config(
    materialized = 'table',
    schema = 'QUARANTINE'  
) }}
with raw_products as (
    select 
        cast (product_key as bigint) as product_key,
        cast (product_subcategory_key as  bigint) as product_subcategory_key,
        cast (product_sku as varchar) as product_sku,

        cast (product_name as varchar) as product_name,
        cast (model_name as varchar)    as model_name,
        cast (product_description as varchar) as product_description,
        cast (product_color as varchar) as product_color,
        cast (product_size as varchar) as product_size,
        cast (product_style as varchar) as product_style,
        cast (product_cost as decimal(18,2)) as product_cost,
        cast (product_price as decimal(18,2)) as product_price
    from {{ source('BRONZE', 'products') }}
)
select 
    product_key,
    product_subcategory_key,
    product_sku,
    product_name,
    model_name,
    product_description,
    product_color,
    product_size,
    product_style,
    product_cost,
    product_price,
    CASE 
        WHEN product_key IS NULL THEN 'Lỗi: Thiếu khóa chính product_key'
        WHEN product_name IS NULL OR TRIM(product_name) = '' THEN 'Lỗi: Tên sản phẩm bị rỗng'
        WHEN product_price <= 0 THEN 'Lỗi: Giá bán <= 0'
        WHEN product_cost > product_price THEN 'Cảnh báo: Giá vốn lớn hơn giá bán (bán lỗ)'
        ELSE 'Lỗi chất lượng dữ liệu khác'
    END AS error_reason,
    
    CURRENT_TIMESTAMP() AS quarantined_at
from raw_products
WHERE product_key IS NULL
   OR product_name IS NULL 
   OR TRIM(product_name) = ''
   OR product_price <= 0
   OR product_cost > product_price

