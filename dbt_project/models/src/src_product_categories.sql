with raw_customer_categories as (
    select 
        *
    from {{ source('BRONZE', 'PRODUCT_CATEGORIES') }}
)
select 
    *
from raw_customer_categories
