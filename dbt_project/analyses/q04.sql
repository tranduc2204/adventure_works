
--Q4: Total returned quantity
--
--Give me the total quantity of returned goods across the whole dataset. One number.
--
--- Head of Quality


with return_table as (
    select *
    from {{ ref('fct_returns') }}
)
, dim_calendar as (
    select *
    from {{ ref('dim_calendar') }}
)
select 
    count (*) as return_records,
    min (c.FULL_DATE) as min_date,
    max (c.FULL_DATE) as max_date,
    sum (r.return_quantity ) as total_quantity_return
from return_table  r
left join dim_calendar c
on r.RETURN_DATE_KEY = c.DATE_KEY

