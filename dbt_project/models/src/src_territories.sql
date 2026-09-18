WITH raw_territories AS (
    SELECT 
        cast (sales_territory_key as bigint) AS sales_territory_key,
        cast (region as varchar) AS region,
        cast (country as varchar) AS country,
        cast (continent as varchar) AS continent
    FROM {{ source('BRONZE', 'TERRITORIES') }}
)

SELECT 
    sales_territory_key,
    region,
    country,
    continent
FROM raw_territories

