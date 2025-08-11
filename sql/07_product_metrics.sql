-- Calculate product performance metrics
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.list_price,
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    SUM(o.quantity) as total_quantity_sold,
    SUM(o.total_amount) as total_revenue,
    AVG(o.unit_price) as avg_selling_price,
    (AVG(o.unit_price) - p.unit_cost) as avg_profit_per_unit,
    ((AVG(o.unit_price) - p.unit_cost) / p.unit_cost) * 100 as profit_margin_pct,
    COUNT(DISTINCT o.order_year_month) as months_with_sales,
    MIN(o.order_date) as first_sale_date,
    MAX(o.order_date) as last_sale_date
FROM temp_products p
LEFT JOIN clean_orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.product_name, p.category, p.subcategory, 
         p.brand, p.list_price, p.unit_cost
HAVING total_orders > 0;
