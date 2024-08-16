WITH int_orders_by_state AS (
    SELECT * FROM {{ ref('int_orders_by_state') }}
)

SELECT
    customer_state,
    order_count,
    RANK() OVER (ORDER BY order_count DESC) AS state_rank
FROM int_orders_by_state