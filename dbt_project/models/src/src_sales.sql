with raw_sales as (
    select 

        -- cast (order_date as date) as order_date,
        -- cast (stock_date as date) as stock_date,
        TO_VARCHAR(order_date, 'YYYYMMDD')::INT AS order_date_key,
        TO_VARCHAR(stock_date, 'YYYYMMDD')::INT AS stock_date_key,

        cast (order_number as varchar) as order_number,
        cast (product_key as bigint) as product_key,
        cast (customer_key as bigint) as customer_key,
        cast (territory_key as bigint) as territory_key,
        cast (order_line_item as bigint) as order_line_item,
        cast (order_quantity as bigint) as order_quantity,
    from {{ source('BRONZE', 'sales') }}
)
select 
    order_date_key,
    stock_date_key,
    order_number,
    product_key,
    customer_key,
    territory_key,
    order_line_item,
    order_quantity
from raw_sales

