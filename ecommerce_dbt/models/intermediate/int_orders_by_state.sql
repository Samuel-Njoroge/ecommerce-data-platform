WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    customer_state,
    COUNT(DISTINCT order_id) AS order_count
FROM stg_orders
GROUP BY 1