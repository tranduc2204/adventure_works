

with products_info as (
    select product_scd_key, count (*) sl
    from {{ ref('dim_products') }}
    group by product_scd_key
)
select *
from products_info
where sl >1







