with raw_returns as (
    select 
        -- cast (return_date as date) as return_date,
        TO_VARCHAR(return_date, 'YYYYMMDD')::INT AS return_date_key,
        cast (territory_key as bigint) as territory_key,
        cast (product_key as bigint) as product_key,
        cast (return_quantity as bigint) as return_quantity
    from {{ source('BRONZE', 'returns') }}
)
select 
    return_date_key,
    territory_key,
    product_key,
    return_quantity
from raw_returns