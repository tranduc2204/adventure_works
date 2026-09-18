with raw__dlt_loads as (
    select 
        LOAD_ID,
        SCHEMA_NAME,
        STATUS,
        SCHEMA_VERSION_HASH
    from {{ source('BRONZE', '_DLT_LOADS') }}
)
select 
    LOAD_ID,
    SCHEMA_NAME,
    STATUS,
    SCHEMA_VERSION_HASH
from raw__dlt_loads
