WITH orders AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),
order_items AS (
    SELECT * FROM {{ source('raw', 'order_items') }}
),
customers AS (
    SELECT * FROM {{ source('raw', 'customers') }}
)

SELECT
    o.order_id,
    o.customer_id,
    c.customer_state,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    oi.product_id,
    oi.price,
    oi.freight_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN customers c ON o.customer_id = c.customer_id