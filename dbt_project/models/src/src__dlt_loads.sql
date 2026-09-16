with raw__dlt_loads as (
    select 
        *
    from {{ source('BRONZE', '_DLT_LOADS') }}
)
select 
    *
from raw__dlt_loads
