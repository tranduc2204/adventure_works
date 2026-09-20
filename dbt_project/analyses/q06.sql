

--Q6: Gross margin by subcategory
--I need the gross margin (revenue − cost of goods) by subcategory. 
--Which subcategory is the most profitable?
--
--- CFO



select ps.subcategory_name, count (s.product_key) as total_products, sum (s.order_quantity) as total_quantity
, sum (s.order_quantity * p.product_price - s.order_quantity * p.product_cost  ) as marginn
from adventure_works.sales s
left join adventure_works.products p 
	on s.product_key  = p.product_key 
left join adventure_works.product_subcategories ps 
	on p.product_subcategory_key  = ps.product_subcategory_key 
group by ps.subcategory_name
order by sum (s.order_quantity * p.product_price - s.order_quantity * p.product_cost  ) desc
