-- Q5: Products by color

-- I need the number of products by color + % of total - to understand color preference in the catalog.

-- - Head of Merchandising


with total_product as (
	select count (*) as total_products
	from adventure_works.products p 
)
select p.product_color,count (p.product_key ) as quantity_products, 
avg (p.product_price ) as avg_prices , 
round ((cast (count (*) as real) /tp.total_products) * 100,2)
from adventure_works.products p 
cross join total_product tp
group by p.product_color , tp.total_products




