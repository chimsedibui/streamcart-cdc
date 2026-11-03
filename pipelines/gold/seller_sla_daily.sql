-- Packing SLA per shop and day: the clock starts when an order becomes TO_SHIP (online: paid_at; COD orders start in
-- TO_SHIP, so created_at) and the seller must have it ready (READY_TO_SHIP, ready_at) by confirm_by, else the source
-- cancels it (seller_sla_breach)
WITH due AS (
    SELECT
        shop_id,
        shop_name,
        CAST(coalesce(paid_at, created_at) AT TIME ZONE 'Asia/Ho_Chi_Minh' AS date)  AS sla_date,
        date_diff('second', coalesce(paid_at, created_at), ready_at)                AS seconds_to_pack,
        CASE
            WHEN ready_at IS NOT NULL AND ready_at <= confirm_by THEN 'on_time'
            WHEN ready_at IS NOT NULL THEN 'late'
            WHEN cancel_reason = 'seller_sla_breach' THEN 'sla_cancelled'
            WHEN cancelled_at IS NOT NULL THEN 'cancelled'
            ELSE 'pending'
        END                                                     AS outcome
    FROM gold.fct_orders
    WHERE confirm_by IS NOT NULL
)
SELECT
    shop_id,
    shop_name,
    sla_date,
    count(*)                                                    AS orders_due,
    count_if(outcome = 'on_time')                               AS packed_on_time,
    count_if(outcome = 'late')                                  AS packed_late,
    count_if(outcome = 'sla_cancelled')                         AS sla_cancelled,
    count_if(outcome = 'cancelled')                             AS other_cancelled,
    count_if(outcome = 'pending')                               AS pending,
    CAST(count_if(outcome = 'on_time') AS double)
        / nullif(count_if(outcome IN ('on_time', 'late', 'sla_cancelled')), 0)  AS on_time_rate,
    avg(seconds_to_pack) / 3600.0                               AS avg_hours_to_pack
FROM due
GROUP BY shop_id, shop_name, sla_date
