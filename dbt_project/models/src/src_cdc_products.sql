WITH raw_cdc_products AS (
    SELECT
        *,
        CASE 
            WHEN _OPERATION = 1 THEN 'DELETE'
            WHEN _OPERATION = 2 THEN 'INSERT'
            WHEN _OPERATION = 3 THEN 'UPDATE_BEFORE'
            WHEN _OPERATION = 4 THEN 'UPDATE_AFTER'
            ELSE 'UNKNOWN'
        END AS cdc_operation_desc
    FROM {{ source('BRONZE', 'dbo_products_ct') }}
)

SELECT * FROM raw_cdc_products
