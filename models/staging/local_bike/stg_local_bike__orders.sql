SELECT
    order_id
    , customer_id
    , store_id
    , staff_id
    , order_status
    , order_date
    , EXTRACT(WEEK FROM order_date) AS order_week
    , EXTRACT(MONTH FROM order_date) AS order_month
    , EXTRACT(YEAR FROM order_date) as order_year
    , required_date
    , shipped_date
FROM {{ source('local_bike_dataset', 'orders') }}