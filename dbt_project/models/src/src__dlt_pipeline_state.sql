with raw__dlt_pipeline_state as (
    select 
        *
    from {{ source('BRONZE', '_DLT_PIPELINE_STATE') }}
)
select 
    *
from raw__dlt_pipeline_state