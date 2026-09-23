
select 
    product_key,
    product_price,
    product_cost
from {{ ref('dim_products') }}
where product_price < product_cost






