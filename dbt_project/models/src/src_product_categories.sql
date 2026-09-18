with raw_customer_categories as (
    select 
        cast (product_category_key as bigint)  as product_category_key,
        cast (category_name as varchar) as category_name
    from {{ source('BRONZE', 'PRODUCT_CATEGORIES') }}
)
select 
    product_category_key,
    category_name
from raw_customer_categories
