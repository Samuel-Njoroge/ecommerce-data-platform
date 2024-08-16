WITH int_avg_delivery_time AS (
    SELECT * FROM {{ ref('int_avg_delivery_time') }}
)

SELECT
    customer_state,
    avg_delivery_time_days,
    CASE
        WHEN avg_delivery_time_days <= 3 THEN 'Fast'
        WHEN avg_delivery_time_days <= 7 THEN 'Medium'
        ELSE 'Slow'
    END AS delivery_speed_category
FROM int_avg_delivery_time