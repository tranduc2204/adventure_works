
--Q2: Which category has the highest revenue

--I need revenue by category (Accessories, Bikes, Clothing, ...). Sorted descending.

--- Head of Merchandising



-- select pc.category_name , count  (distinct s.order_number ) orders, 
-- sum (p.product_price ) as units_sold, 
-- sum (s.order_quantity * p.product_price ) as revenue
-- from adventure_works.sales s 
-- left join adventure_works.products p 
-- 	on s.product_key  = p.product_key 
-- left join adventure_works.product_subcategories ps 
-- 	on ps.product_subcategory_key  = p.product_subcategory_key 
-- left join adventure_works.product_categories pc 
-- 	on ps.product_category_key  = pc.product_category_key 
-- group by pc.category_name

with dim_products as (
    select *
    from {{ ref ('dim_products') }}
), fct_sales as (
    select *
    from {{ ref('fct_sales') }}
), dim_calendar(
    select *
    from {{ ref('dim_calendar') }}
)
select 
    dp.category_name, count (distinct s.order_number) as orders, 
    sum (dp.product_price) as units_sold,
    sum (order_quantity * product_price) as revenues
from fct_sales s 
left join dim_calendar cal 
    on cal.date_key = ORDER_DATE_KEY
left join dim_products dp
on s.product_key = dp.product_key
and cal.full_date >= dp.DBT_VALID_FROM 
and (cal.full_date < dp.DBT_VALID_TO or dp.DBT_VALID_TO is NULL)
group by dp.category_name















