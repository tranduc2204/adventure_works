-- Q1: Total gross sales
-- Give me the total gross revenue (selling price × quantity) across the entire dataset.

-- - CFO

-- select count (distinct order_number) as orders, sum (s.order_quantity ) as  all_quantity,
-- sum (s.order_quantity  * p.product_price )
-- from adventure_works.sales s 
-- left join adventure_works.products p 
-- 	on s.product_key  = p.product_key 


with sales as (
    select *
    from {{ ref('fct_sales') }}
), products as (
    select *
    from {{ ref('dim_products') }}
)
select 
    count (distinct order_number) as orders, 
    sum (s.order_quantity ) as  all_quantity,
    sum (s.order_quantity  * p.product_price ) as revenue
from sales s
left join products p
on s.product_key = p.product_key























