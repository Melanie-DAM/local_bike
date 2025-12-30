WITH staffs AS (
    SELECT 
    *
    FROM {{ ref ('stg_local_bike__staffs')}} 
)

, orders AS (
    SELECT 
    *
    FROM {{ ref ('stg_local_bike__orders')}}
)

, staff_orders AS ( 
    SELECT 
    s.staff_id
    , s.staff_name
    , s.store_id
    , s.manager_id
    , o.order_id
    , o.order_date
    , o.order_year
    , o.shipped_date
    , o.required_date
    FROM staffs s
    INNER JOIN orders o ON s.staff_id = o.staff_id
    ) 
    
SELECT 
    staff_id
    , staff_name
    , store_id
    , manager_id
    , order_date
    , order_year
    , COUNT(order_id) AS total_orders_processed

FROM staff_orders 
GROUP BY 
    staff_id
    , staff_name
    , store_id
    , manager_id
    , order_date
    , order_year