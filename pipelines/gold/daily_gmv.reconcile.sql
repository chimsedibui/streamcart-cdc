-- The same metric computed on the source (PostgreSQL, read-only role app_reader): gold must match it once the
-- source is quiet (the simulator stopped and the business clock slowed, see quality/reconcile.py)
SELECT
    (created_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date                      AS order_date,
    count(*)                                                                AS orders,
    count(*) FILTER (WHERE status <> 'CANCELLED')                           AS valid_orders,
    coalesce(sum(subtotal) FILTER (WHERE status <> 'CANCELLED'), 0)         AS gmv,
    coalesce(sum(total) FILTER (WHERE status <> 'CANCELLED'), 0)            AS buyer_paid
FROM ecommerce.orders
GROUP BY 1
