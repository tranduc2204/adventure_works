{{ config(
    materialized='table',
    schema='src',
    alias='src_products')
}}
with raw_products as (
    select 
        *
    from {{ source('BRONZE', 'products') }}
)
select 
    *
from raw_products


