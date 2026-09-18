{% snapshot snap_products %}
{{
    config(
      target_schema='BRONZE_STAGING',
      unique_key='product_key',
      strategy='check',
      check_cols=['product_price', 'product_cost']
    )
}}


select 
    product_key,
    product_subcategory_key,
    product_sku,
    product_name,
    model_name,
    product_description,
    product_color,
    product_size,
    product_style,
    product_cost,
    product_price
from {{ ref('src_products') }}
{% endsnapshot %}