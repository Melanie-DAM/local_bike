WITH orders_perf AS (
    SELECT 
    *
    -- Status labels
    , CASE ord.order_status
        WHEN 1 THEN 'Pending'
        WHEN 2 THEN 'Processing'
        WHEN 3 THEN 'Rejected'
        WHEN 4 THEN 'Completed'
        ELSE 'Other'
      END AS status_description
    FROM {{ ref('int_local_bike__orders_performances') }}
)

, stores AS (
    SELECT
    s.store_name
    , s.store_city
    , s.store_state
    , s.store_zip_code
    FROM {{ ref('stg_local_bike__stores') }}
)

, staffs AS (
    SELECT
    *
    FROM {{ ref('stg_local_bike__staffs') }}
)


SELECT *
FROM orders_perf ord
LEFT JOIN store s ON ord.store_id = s.store_id
LEFT JOIN staffs st ON ord.staff_id = st.staff_id