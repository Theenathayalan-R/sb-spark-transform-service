-- Load raw orders data
SELECT 
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    total_amount,
    order_status,
    payment_method,
    shipping_address
FROM raw_data.orders
WHERE order_date >= DATE('2023-01-01')
  AND order_status IN ('completed', 'shipped', 'delivered');
