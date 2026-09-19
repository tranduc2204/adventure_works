WITH date_spine AS (
    SELECT 
        DATEADD(
            DAY, 
            ROW_NUMBER() OVER (ORDER BY NULL) - 1, 
            '2000-01-01'::DATE
        ) AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 15000))
)

SELECT 
    CAST(date AS DATETIME) AS date,
    NULL AS _dlt_load_id,
    NULL AS _dlt_id
FROM date_spine
WHERE date <= CURRENT_DATE()
