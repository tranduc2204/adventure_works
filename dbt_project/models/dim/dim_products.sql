

with snap_product as (
    select * from {{ ref('snap_products') }}
),
SRC_PRODUCT_SUBCATEGORIES as (
    select * from {{ ref('src_product_subcategories') }}
),
SRC_PRODUCT_CATEGORIES as (
    select * from {{ ref('src_product_categories') }}  
)

select 
    p.product_key,
    p.product_subcategory_key,
    ps.subcategory_name,
    pc.product_category_key,
    pc.category_name,
    p.product_sku,
    p.product_name,
    p.model_name,
    p.product_description,
    p.product_color,
  
    NULLIF(p.product_size, '0') AS product_size, 
    CASE 
        WHEN p.product_size IN ('S', 'M', 'L', 'XL') THEN 'Clothing'
        WHEN TRY_CAST(p.product_size AS INT) > 0 THEN 'Frame (cm)'
        ELSE 'No Size'
    END AS product_size_type,
   
    CASE 
        WHEN p.product_style = 'U' THEN 'Unisex'
        WHEN p.product_style = 'W' THEN 'Women'
        WHEN p.product_style = 'M' THEN 'Men'
        WHEN p.product_style = '0' OR product_style IS NULL THEN 'N/A'  
        ELSE p.product_style
    END AS product_style,
    p.product_cost,
    p.product_price,
    p.dbt_updated_at,
    p.dbt_valid_from,
    p.dbt_valid_to
from snap_product p
left join SRC_PRODUCT_SUBCATEGORIES ps
on p.product_subcategory_key = ps.product_subcategory_key
left join SRC_PRODUCT_CATEGORIES pc 
on pc.product_category_key = ps.product_category_key

