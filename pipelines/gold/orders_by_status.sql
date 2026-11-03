-- The order book right now: how many orders, and how much money, sit in each status
SELECT
    status,
    count(*)                                                AS orders,
    sum(total)                                              AS total_value,
    CAST(count(*) AS double) / sum(count(*)) OVER ()        AS share
FROM gold.fct_orders
GROUP BY status
