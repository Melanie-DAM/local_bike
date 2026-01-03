WITH orders_perf AS (
    SELECT 
    *
    -- Status labels
    , CASE order_status
        WHEN 1 THEN 'Pending'
        WHEN 2 THEN 'Processing'
        WHEN 3 THEN 'Rejected'
        WHEN 4 THEN 'Completed'
        ELSE 'Other'
      END AS order_status_description
    FROM {{ ref('int_local_bike__orders_performances') }}
)

, stores AS (
    SELECT
    store_id
    , store_name
    , store_city
    , store_state
    , store_zip_code
    FROM {{ ref('stg_local_bike__stores') }}
)

, staffs AS (
    SELECT
    staf.staff_id
    , staf.staff_name
    , staf.manager_id
    , man.staff_name as manager_name
    , staf.store_id
    FROM {{ ref('stg_local_bike__staffs') }} staf 
    LEFT JOIN {{ ref ('stg_local_bike__staffs')}} man ON staf.manager_id = man.staff_id

)


SELECT 
    ord.order_id
    , ord.customer_id

    , ord.order_status
    , ord.order_status_description
    , ord.order_date
    , ord.required_date
    , ord.shipped_date
    , ord.processing_time_days
    , ord.days_diff_from_required
    , ord.is_late_shipping

    , ord.store_id
    , s.store_name
    , s.store_city
    , s.store_state
    , s.store_zip_code

    , ord.staff_id
    , st.staff_name
    , st.manager_id
    , st.manager_name

    , ord.total_order_amount
    , ord.total_order_amount_net
    , ord.total_items_count

FROM orders_perf ord
LEFT JOIN stores s ON ord.store_id = s.store_id
LEFT JOIN staffs st ON ord.staff_id = st.staff_id