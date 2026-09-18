with raw_product_subcategories as (
    select 
        cast (product_subcategory_key as bigint)  as product_subcategory_key,
        cast (subcategory_name as varchar)  as subcategory_name,
        cast (product_category_key as bigint) as product_category_key
    from {{ source('BRONZE', 'PRODUCT_SUBCATEGORIES') }}
)
select 
    product_subcategory_key,
    subcategory_name,
    product_category_key
from raw_product_subcategories
