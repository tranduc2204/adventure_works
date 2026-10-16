
{{ config(
    materialized = 'incremental',
    unique_key = ['order_number', 'order_line_item'],
    incremental_strategy = 'merge', 


    post_hook = "
        MERGE INTO {{ this }} target
        USING {{ ref('dim_customers') }} source
            ON target.source_customer_id = source.source_customer_id
        WHEN MATCHED AND target.customer_key = -1 THEN
            UPDATE SET target.customer_key = source.customer_key;
    "
  

) }}

WITH base_sales AS (
    -- Dữ liệu snapshot ban đầu
    SELECT 
        s.order_number,
        s.order_line_item,
        s.order_date_key,
        s.stock_date_key,
        COALESCE(p.product_scd_key, '-1') AS product_scd_key, 
        s.product_key,
        s.customer_key,
        s.territory_key,
        s.order_quantity,
        FALSE AS is_deleted
    FROM {{ ref('stg_sqlserver__sales') }} s
    LEFT JOIN {{ ref('dim_products') }} p
        ON s.product_key = p.product_key
       AND s.order_date_key >= p.dbt_valid_from 
       AND (s.order_date_key < p.dbt_valid_to OR p.dbt_valid_to IS NULL)
),

cdc_sales AS (
 
    SELECT 
        s.order_number,
        s.order_line_item,
        s.order_date_key,
        s.stock_date_key,
        COALESCE(p.product_scd_key, '-1') AS product_scd_key, 
        s.product_key,
        s.customer_key,
        s.territory_key,
        s.order_quantity,
        s.is_deleted
    FROM {{ ref('stg_sqlserver__sales_cdc') }} s 
    LEFT JOIN {{ ref('dim_products') }} p
        on s.product_key = p.product_key
    AND s.order_date_key >= p.dbt_valid_from 
    AND (s.order_date_key < p.dbt_valid_to OR p.dbt_valid_to IS NULL)

)

{% if not is_incremental() %}
    SELECT * FROM base_sales

{% else %}
  
    SELECT * FROM cdc_sales
    WHERE is_deleted = FALSE

{% endif %}