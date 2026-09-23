


select 
    customer_key,
    prefix,
    first_name, 
    last_name,
    birth_date,
    case 
        when marital_status = 'S' then 'Single'
        when marital_status = 'M' then 'Married'
        when marital_status = 'D' then 'Divorced'
        when marital_status = 'W' then 'Widowed'
        else 'N/A' 
    end as marital_status,
    case 
        when gender = 'M' then 'Male'
        when gender = 'F' then 'Female'
        else 'N/A'  
    end as gender,
    email_address,
    yearly_income,
    total_children,
    education_level,
    occupation,
    CASE 
        WHEN home_owner = 'Y' THEN TRUE 
        WHEN home_owner = 'N' THEN FALSE 
        ELSE FALSE 
    END AS is_home_owner
from {{ ref('src_customers') }}

-- UNION ALL 

-- -- select 
-- --     -1 as customer_key





















