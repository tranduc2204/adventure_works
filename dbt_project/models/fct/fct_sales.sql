
{{ config(
    materialized = 'incremental',
    unique_key = ['order_number', 'order_line_item'],
    incremental_strategy = 'merge'
) }}

WITH base_sales AS (
    -- Dữ liệu snapshot ban đầu
    SELECT 
        order_number,
        order_line_item,
        order_date_key,
        stock_date_key,
        product_key,
        customer_key,
        territory_key,
        order_quantity,
        FALSE AS is_deleted
    FROM {{ ref('src_sales') }}
),

cdc_sales AS (
 
    SELECT 
        order_number,
        order_line_item,
        order_date_key,
        stock_date_key,
        product_key,
        customer_key,
        territory_key,
        order_quantity,
        is_deleted
    FROM {{ ref('src_cdc_sales') }}
)

{% if not is_incremental() %}
    SELECT * FROM base_sales

{% else %}
  
    SELECT * FROM cdc_sales
    WHERE is_deleted = FALSE

{% endif %}