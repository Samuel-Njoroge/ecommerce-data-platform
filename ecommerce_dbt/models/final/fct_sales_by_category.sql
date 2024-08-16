WITH int_sales_by_category AS (
    SELECT * FROM {{ ref('int_sales_by_category') }}
)

SELECT
    product_category_name,
    order_date,
    total_sales,
    order_count,
    total_sales / order_count AS average_order_value
FROM int_sales_by_category