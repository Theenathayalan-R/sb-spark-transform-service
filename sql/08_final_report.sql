-- Create consolidated business report
WITH monthly_summary AS (
    SELECT 
        order_year_month,
        COUNT(DISTINCT customer_id) as active_customers,
        COUNT(DISTINCT order_id) as total_orders,
        SUM(total_amount) as monthly_revenue,
        AVG(total_amount) as avg_order_value
    FROM clean_orders
    GROUP BY order_year_month
),
category_performance AS (
    SELECT 
        product_category,
        SUM(total_revenue) as category_revenue,
        SUM(total_quantity_sold) as category_quantity,
        COUNT(DISTINCT product_id) as products_in_category
    FROM product_metrics
    GROUP BY product_category
)
SELECT 
    'Monthly Trends' as report_section,
    order_year_month as dimension,
    CAST(monthly_revenue as VARCHAR) as metric_value,
    'Revenue' as metric_name
FROM monthly_summary

UNION ALL

SELECT 
    'Category Performance' as report_section,
    product_category as dimension,
    CAST(category_revenue as VARCHAR) as metric_value,
    'Revenue' as metric_name
FROM category_performance

UNION ALL

SELECT 
    'Customer Segments' as report_section,
    value_segment as dimension,
    CAST(COUNT(*) as VARCHAR) as metric_value,
    'Customer Count' as metric_name
FROM customer_metrics
GROUP BY value_segment;
