WITH order_perf AS (
    SELECT 
    *
    FROM {{ ref('int_local_bike__order_details')}}
)

, dim_items AS (
    SELECT 
    *
    FROM {{ ref('int_local_bike__dim_items')}}
)

, staff AS (
    SELECT
    *
    FROM {{ ref('int_local_bike__staff_performances')}}
)

, stock AS (
    SELECT
    *
    FROM {{ ref('int_local_bike__stock_inventory')}}
)

SELECT 
    op.order_id
    , op.order_date
    , op.shipped_date
    , op.required_date
    , op.order_status
    , op.customer_id
    , op.customer_name
    , op.product_id
    , di.product_name
    , di.category_id
    , di.category_name
    , di.brand_id
    , di.brand_name
    , s.staff_id
    , s.staff_name

    , SUM(op.total_items) AS total_items
    , SUM(op.total_revenue) AS total_revenue
    , SUM(op.total_net_revenue) AS total_net_revenue


FROM order_perf op 
LEFT JOIN dim_items di ON op.product_id = di.product_id
LEFT JOIN staff s ON s.staff_id = op.staff_id
LEFT JOIN stock st ON st.store_id = op.store_id

GROUP BY ALL