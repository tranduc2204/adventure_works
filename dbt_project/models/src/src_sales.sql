{{ config(
    materialized='table',
    schema='src',
    alias='src_sales')
}}
with raw_sales as (
    select 
        *
    from {{ source('BRONZE', 'sales') }}
)
select
    *
from raw_sales