with raw_product_subcategories as (
    select 
        *
    from {{ source('BRONZE', 'PRODUCT_SUBCATEGORIES') }}
)
select 
    *
from raw_product_subcategories
