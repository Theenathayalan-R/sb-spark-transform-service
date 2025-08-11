-- Load product catalog data
SELECT 
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    unit_cost,
    list_price,
    product_status,
    launch_date
FROM raw_data.products
WHERE product_status = 'active';
