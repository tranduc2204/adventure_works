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
    product_price
from raw_products
WHERE product_key IS NOT NULL
   AND product_name IS NOT NULL 
   AND TRIM(product_name) <> ''
   AND product_price > 0
   AND product_cost <= product_price


