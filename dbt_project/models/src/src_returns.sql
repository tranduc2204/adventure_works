with raw_returns as (
    select 
        *
    from {{ source('BRONZE', 'returns') }}
)
select 
    *
from raw_returns