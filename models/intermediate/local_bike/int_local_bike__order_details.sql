WITH orders AS (
    SELECT
    *
    FROM {{ ref ('stg_local_bike__orders') }}
)

, items AS (
    SELECT 
    *
    FROM {{ ref ('stg_local_bike__order_items') }}
)

, products AS (
    SELECT 
    *
    FROM {{ ref ('stg_local_bike__products') }}
)

SELECT 
    o.order_id
    , o.customer_id
    , o.store_id
    , o.staff_id
    , o.order_status
    , o.order_date
    , o.required_date
    , o.shipped_date
    , i.product_id
    , p.product_name
    , i.unit_price

    , SUM(i.quantity) AS total_items
    , SUM(i.order_item_revenue) AS total_revenue
    , SUM(i.order_item_net_revenue) AS total_net_revenue

FROM orders o
LEFT JOIN items i ON o.order_id = i.order_id
LEFT JOIN products p ON i.product_id = p.product_id

GROUP BY
  o.order_id,
  o.customer_id,
  o.store_id,
  o.staff_id,
  o.order_status,
  o.order_date,
  o.required_date,
  o.shipped_date,
  i.product_id,
  p.product_name,
  i.unit_price
