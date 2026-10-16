select 
        RETURN_DATE_KEY,
        TERRITORY_KEY,
        PRODUCT_KEY,
        RETURN_QUANTITY
from {{ ref('stg_sqlserver__returns')  }}