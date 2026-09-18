with raw_sales as (
    select 
        *
    from {{ source('BRONZE', 'sales') }}
), raw_sales_cdc as (
    select 
        *
    from {{ source('BRONZE', 'sales_cdc') }}
)
select 
    *
from raw_sales

