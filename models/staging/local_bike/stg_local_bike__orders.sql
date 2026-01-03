WITH orders AS (
    SELECT
    *
    FROM {{ source('local_bike_dataset', 'orders') }}
)
    
SELECT
    SAFE_CAST(order_id AS INT64) AS order_id
    , SAFE_CAST(customer_id AS INT64) AS customer_id
    , SAFE_CAST(store_id AS INT64) AS store_id
    , SAFE_CAST(staff_id AS INT64) AS staff_id
    
    , SAFE_CAST(order_status AS INT64) AS order_status
    
    , SAFE_CAST(order_date AS DATE) AS order_date
    , SAFE_CAST(required_date AS DATE) AS required_date
    , SAFE_CAST(shipped_date AS DATE) AS shipped_date
FROM orders