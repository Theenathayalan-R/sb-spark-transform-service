-- Clean and standardize customer data
SELECT 
    customer_id,
    TRIM(UPPER(first_name)) as first_name,
    TRIM(UPPER(last_name)) as last_name,
    LOWER(TRIM(email)) as email,
    REGEXP_REPLACE(phone, '[^0-9]', '') as phone_clean,
    TRIM(address) as address,
    TRIM(UPPER(city)) as city,
    UPPER(state) as state,
    REGEXP_REPLACE(zip_code, '[^0-9]', '') as zip_code,
    registration_date,
    COALESCE(customer_segment, 'Unknown') as customer_segment,
    CASE 
        WHEN registration_date >= DATE('2024-01-01') THEN 'New'
        WHEN registration_date >= DATE('2023-01-01') THEN 'Recent'
        ELSE 'Existing'
    END as customer_age_group
FROM temp_customers
WHERE email IS NOT NULL 
  AND email LIKE '%@%'
  AND LENGTH(first_name) > 0
  AND LENGTH(last_name) > 0;
