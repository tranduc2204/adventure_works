
select 
    product_key,
    product_price,
    product_cost
from {{ rerf('dim_products') }}
where product_price < product_cost






