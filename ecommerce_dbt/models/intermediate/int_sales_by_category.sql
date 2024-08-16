WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
stg_products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    p.product_category_name,
    DATE(o.order_purchase_timestamp) AS order_date,
    SUM(o.price) AS total_sales,
    COUNT(DISTINCT o.order_id) AS order_count
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
GROUP BY 1, 2