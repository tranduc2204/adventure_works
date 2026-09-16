with raw_calendar as (
    select
        *
    from {{ source('BRONZE', 'calendar') }}
)
select 
    date,
    _DLT_LOAD_ID,
    _DLT_ID
from raw_calendar
