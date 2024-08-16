WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    customer_state,
    AVG(DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY)) AS avg_delivery_time_days
FROM stg_orders
WHERE order_status = 'delivered'
GROUP BY 1