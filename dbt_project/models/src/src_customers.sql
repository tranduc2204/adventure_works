with raw_customers as (
    select 
        *
    from {{ source('BRONZE', 'customers') }}
)
select 
    *
from raw_customers

