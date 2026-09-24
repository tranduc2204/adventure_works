
{{ config(
    materialized = 'incremental',
    unique_key = ['order_number', 'order_line_item'],
    incremental_strategy = 'merge'


    post_hook = "
        MERGE INTO {{ this }} target
        USING {{ ref('dim_customers') }} source
            ON target.source_customer_id = source.source_customer_id
        WHEN MATCHED AND target.customer_key = -1 THEN
            UPDATE SET target.customer_key = source.customer_key;
    "
    -- có thể tách airflow tạo 1 task trong airflow
    
    -- # Trong dag_daily_dimensions.py của Airflow:
    -- heal_customers_task = SQLExecuteQueryOperator(
    --     task_id="heal_unresolved_customers_in_fact",
    --     conn_id="snowflake_default",
    --     sql="""
    --         MERGE INTO ADVENTUREWORKS.GOLD.fct_sales target
    --         USING ADVENTUREWORKS.GOLD.dim_customers source
    --             ON target.source_customer_id = source.source_customer_id
    --         WHEN MATCHED AND target.customer_key = -1 THEN
    --             UPDATE SET target.customer_key = source.customer_key;
    --     """
    -- )
    -- # Chạy sau khi nạp xong Dim đêm
    -- dbt_run_dimensions >> heal_customers_task

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
    FROM {{ ref('src_sales') }} s
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
    FROM {{ ref('src_cdc_sales') }} s 
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