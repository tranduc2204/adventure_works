with raw_calendar as (
    select
        cast (date as datetime) as date,
        _DLT_LOAD_ID,
        _DLT_ID
    from {{ source('BRONZE', 'calendar') }}
)
select 
    date,
    _DLT_LOAD_ID,
    _DLT_ID
from raw_calendar
