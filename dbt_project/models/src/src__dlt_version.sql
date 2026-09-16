with raw__dlt_version as (
    select 
        *
    from {{ source('BRONZE', '_DLT_VERSION') }} 
)
select *
from raw__dlt_version 