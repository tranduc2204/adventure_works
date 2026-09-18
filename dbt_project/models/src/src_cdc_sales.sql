

with raw_sales_cdc as (
    select 
        ORDER_NUMBER,
        ORDER_LINE_ITEM,
        ORDER_DATE,
        STOCK_DATE,
       
        PRODUCT_KEY,
        CUSTOMER_KEY,
        TERRITORY_KEY,
        
        ORDER_QUANTITY,
        _operation,
        _start_lsn,
        _seqval,
        ROW_NUMBER() OVER (
            PARTITION BY order_number, order_line_item 
            ORDER BY _start_lsn DESC, _seqval DESC
        ) AS rn
    from {{ source('BRONZE', 'DBO_SALES_CT') }}
)
select 
    order_number,
    order_line_item,
    order_date,
    stock_date,
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














