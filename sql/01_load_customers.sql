-- Load raw customer data from source system
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    zip_code,
    registration_date,
    customer_segment
FROM raw_data.customers
WHERE registration_date >= DATE('2023-01-01')
  AND customer_id IS NOT NULL;
