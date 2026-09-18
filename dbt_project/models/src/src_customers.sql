with raw_customers as (
    select 
        cast (customer_key as bigint) as customer_key,
        cast (prefix as varchar) as prefix,
        cast (first_name as varchar) as first_name, 
        cast (last_name as varchar) as last_name,
        cast (birth_date as date) as birth_date,
        cast (marital_status as varchar) as marital_status,
        cast (gender as varchar) as gender,
        cast (email_address as varchar) as email_address,
        cast (annual_income as decimal(18,2)) as yearly_income,
        cast (total_children as bigint) as total_children,
        cast (education_level as varchar) as education_level,
        cast (occupation as varchar) as occupation,
        cast (home_owner as varchar) as home_owner
    from {{ source('BRONZE', 'customers') }}
)
select 
    customer_key,
    prefix,
    first_name, 
    last_name,
    birth_date,
    marital_status,
    gender,
    email_address,
    yearly_income,
    total_children,
    education_level,
    occupation,
    home_owner
from raw_customers

