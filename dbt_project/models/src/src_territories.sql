WITH raw_territories AS (
    SELECT 
        *
    FROM {{ source('BRONZE', 'TERRITORIES') }}
)

SELECT 
    *
FROM raw_territories

