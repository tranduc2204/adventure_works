

with raw_sales_cdc as (
    select 
        cast (ORDER_NUMBER as varchar(50)) as ORDER_NUMBER,
        cast (ORDER_LINE_ITEM as int) as ORDER_LINE_ITEM,

        TO_VARCHAR(order_date, 'YYYYMMDD')::INT AS order_date_key,
        TO_VARCHAR(stock_date, 'YYYYMMDD')::INT AS stock_date_key,

        cast (PRODUCT_KEY as int) as PRODUCT_KEY,
        cast (CUSTOMER_KEY as int) as CUSTOMER_KEY,
        cast (TERRITORY_KEY as int) as TERRITORY_KEY,
        cast (ORDER_QUANTITY as int) as ORDER_QUANTITY,
        cast (_operation as int) as _operation,
        cast (_start_lsn as varchar(50)) as _start_lsn,
        cast (_seqval as varchar(50)) as _seqval,
        ROW_NUMBER() OVER (
            PARTITION BY order_number, order_line_item 
            ORDER BY _start_lsn DESC, _seqval DESC
        ) AS rn
    from {{ source('BRONZE', 'DBO_SALES_CT') }}
)
select 
    order_number,
    order_line_item,
    order_date_key,
    stock_date_key,
    product_key,
    customer_key,
    territory_key,
    order_quantity,
    _operation,
    _start_lsn,
    CASE 
        WHEN _operation = 1 THEN TRUE 
        ELSE FALSE 
    END AS is_deleted
from raw_sales_cdc
where rn = 1














