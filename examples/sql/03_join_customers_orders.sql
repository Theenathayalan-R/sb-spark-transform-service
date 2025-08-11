-- demo: join customers and orders using placeholders
SELECT c.customer_id, c.name, o.order_id, o.amount
FROM ${load_customers} c
JOIN ${load_orders} o ON c.customer_id = o.customer_id;
