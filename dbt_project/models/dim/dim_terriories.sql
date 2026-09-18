select 
    sales_territory_key,
    region,
    country,
    continent
from {{ ref('src_territories') }} 