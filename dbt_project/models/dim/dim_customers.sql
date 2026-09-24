{{
    config(
        materialized = 'table'
    )
}}

with actual_customers as (
    -- Khách thực tế từ nguồn CRM
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
        END AS is_home_owner,
        -- hỗ trợ alert cho airflow. hôm trước có bao nhiu khách hàng mua hàng trước khi được update data từ crm 
        FALSE AS is_inferred  -- Khách hàng thật, đầy đủ thông tin
    from {{ ref('src_customers') }}
),

inferred_customers as (
    -- bắt các key customer chưa được nạp 
    --- find list customer can't dump later
    select distinct  
        s.customer_key,
        'N/A' as prefix,
        'Pending' as first_name, 
        'Pending' AS last_name,
        '1900-01-01'::DATE AS  birth_date,
        'N/A' AS  marital_status,
        'N/A' AS gender,
        'unknown@adventure-works.com' AS email_address,
        0 AS yearly_income,
        0 AS total_children,
        'N/A' AS education_level,
        'N/A' AS occupation,
        FALSE AS  is_home_owner,
        TRUE AS is_inferred  -- Cờ đánh dấu: Khách này đến từ Fact trước khi có profile!
    from {{ ref('src_sales') }} s
    where s.customer_key is not null 
        and s.customer_key not in (select customer_key from actual_customers )
),
unknown_default_customer as (
    -- khách vãng lai hoặc order bi null customer_key 

    SELECT 
        -1 AS customer_key,
        'N/A' AS prefix,
        'Guest' AS first_name,
        'Unknown' AS last_name,
        '1900-01-01'::DATE AS birth_date,
        'N/A' AS marital_status,
        'N/A' AS gender,
        'guest@adventure-works.com' AS email_address,
        0 AS yearly_income,
        0 AS total_children,
        'N/A' AS education_level,
        'N/A' AS occupation,
        FALSE AS is_home_owner,
        FALSE AS is_inferred

)

SELECT * FROM actual_customers
UNION ALL
SELECT * FROM inferred_customers
UNION ALL
SELECT * FROM unknown_default_customer





















