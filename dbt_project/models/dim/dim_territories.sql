{{ config(
    materialized = 'table'
) }}

with actual_terriories as (
    -- khách thực tế từ nguồn CRM
    select 
        sales_territory_key as territory_key,
        region,
        country,
        continent,
        FALSE AS is_inferred 
    from {{ ref('src_territories') }}
)
, 
inferred_terriories as (
    -- bắt các key terriories chưa được nạp
    -- find list terriories can't dump later
    select distinct 
        t.territory_key ,
        'N/A' as region,
        'N/A' as country, 
        'N/A' as continent,
        TRUE AS is_inferred 
    from {{ ref('src_sales') }} t
    where t.territory_key is not null 
        and t.territory_key not in  (select distinct territory_key 
        from actual_terriories
        where territory_key is not null)
),
unknown_default_terriories as (
    --- bắt case null
    select 
        -1 as territory_key,
        'N/A' as region,
        'N/A' as country, 
        'N/A' as continent,
        FALSE AS is_inferred 
)

SELECT * FROM actual_terriories
UNION ALL
SELECT * FROM inferred_terriories
UNION ALL
SELECT * FROM unknown_default_terriories
