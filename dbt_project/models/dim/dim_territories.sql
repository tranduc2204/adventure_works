{{ config(
    materialized = 'table'
) }}

select 
    sales_territory_key as territory_key,
    region,
    country,
    continent
from {{ ref('src_territories') }}

