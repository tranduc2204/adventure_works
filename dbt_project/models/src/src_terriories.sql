with raw_territories as (
    select 
        *
    from {{ source('BRONZE', 'TERRITORIES') }}
)
select 
    *
from raw_territories