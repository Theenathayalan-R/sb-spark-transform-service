-- Clean orders data and add derived fields
SELECT 
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.order_status,
    o.payment_method,
    p.category as product_category,
    p.brand as product_brand,
    (o.unit_price * o.quantity) as calculated_total,
    ABS(o.total_amount - (o.unit_price * o.quantity)) as amount_discrepancy,
    EXTRACT(YEAR FROM o.order_date) as order_year,
    EXTRACT(MONTH FROM o.order_date) as order_month,
    EXTRACT(QUARTER FROM o.order_date) as order_quarter,
    DATE_FORMAT(o.order_date, '%Y-%m') as order_year_month
FROM temp_orders o
LEFT JOIN temp_products p ON o.product_id = p.product_id
WHERE o.quantity > 0 
  AND o.unit_price > 0
  AND o.total_amount > 0
  AND ABS(o.total_amount - (o.unit_price * o.quantity)) < 0.01;
