

{{
    config ( 
        severity = "warn"
    )
}}

select *
from {{ ref('dim_customers')}}
where birth_date >= CURRENT_DATE()









