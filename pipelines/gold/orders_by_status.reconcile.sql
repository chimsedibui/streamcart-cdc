SELECT status, count(*) AS orders, sum(total) AS total_value
FROM ecommerce.orders
GROUP BY status
