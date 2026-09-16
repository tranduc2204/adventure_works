with raw_products as (
    select 
        *
    from {{ source('BRONZE', 'products') }}
)
select 
    *
from raw_products


