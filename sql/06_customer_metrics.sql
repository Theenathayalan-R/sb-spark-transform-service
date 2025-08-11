-- Calculate customer-level metrics
WITH customer_order_stats AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.customer_segment,
        c.customer_age_group,
        c.registration_date,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(o.total_amount) as total_spent,
        AVG(o.total_amount) as avg_order_value,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date,
        COUNT(DISTINCT o.product_category) as categories_purchased,
        COUNT(DISTINCT DATE_FORMAT(o.order_date, '%Y-%m')) as active_months
    FROM clean_customers c
    LEFT JOIN clean_orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name, c.email, 
             c.customer_segment, c.customer_age_group, c.registration_date
)
SELECT 
    *,
    CASE 
        WHEN total_orders = 0 THEN 'No Orders'
        WHEN total_orders = 1 THEN 'One-time'
        WHEN total_orders <= 5 THEN 'Occasional'
        WHEN total_orders <= 10 THEN 'Regular'
        ELSE 'Frequent'
    END as purchase_frequency_segment,
    CASE 
        WHEN total_spent = 0 THEN 'No Spend'
        WHEN total_spent < 100 THEN 'Low Value'
        WHEN total_spent < 500 THEN 'Medium Value'
        WHEN total_spent < 1000 THEN 'High Value'
        ELSE 'Premium'
    END as value_segment,
    DATE_DIFF('day', last_order_date, CURRENT_DATE) as days_since_last_order
FROM customer_order_stats;
