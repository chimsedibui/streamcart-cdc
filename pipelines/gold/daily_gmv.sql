-- Per business day: orders placed, and GMV over the orders that were not cancelled (cancelling is only possible
-- before shipping; returned orders still count, NMV would subtract them)
SELECT
    order_date,
    count(*)                                                            AS orders,
    count_if(is_cancelled)                                              AS cancelled_orders,
    count_if(NOT is_cancelled)                                          AS valid_orders,
    count(DISTINCT customer_id) FILTER (WHERE NOT is_cancelled)         AS buyers,
    coalesce(sum(subtotal) FILTER (WHERE NOT is_cancelled), 0)          AS gmv,
    coalesce(sum(total) FILTER (WHERE NOT is_cancelled), 0)             AS buyer_paid,
    coalesce(sum(shop_discount + platform_discount) FILTER (WHERE NOT is_cancelled), 0) AS discounts,
    CAST(sum(total) FILTER (WHERE NOT is_cancelled) AS double)
        / nullif(count_if(NOT is_cancelled), 0)                         AS aov,
    CAST(count_if(is_cancelled) AS double) / count(*)                   AS cancel_rate
FROM gold.fct_orders
GROUP BY order_date
