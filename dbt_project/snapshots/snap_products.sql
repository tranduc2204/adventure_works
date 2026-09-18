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
  * 
from {{ ref('src_products') }}
{% endsnapshot %}