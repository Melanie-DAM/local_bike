WITH staff AS (
    SELECT
        staff_id
        , staff_name
        , manager_id
        , manager_name
        , store_id
    FROM {{ ref('stg_local_bike__staffs') }} 
)

, staff_orders AS (
    SELECT 
        staff_id
        , store_id
        , order_date
        --, EXTRACT(YEAR FROM order_date) AS order_year
        , COUNT(order_id) AS total_orders
        , ROUND(SUM(total_order_amount),2) AS total_revenue
        , ROUND(AVG(processing_time_days),2) AS avg_processing_time
    FROM {{ ref('int_local_bike__orders_performances') }}
    WHERE order_status = 4 -- ventes complétées
    GROUP BY 1, 2, 3
)

SELECT
    s.staff_id
    , s.staff_name
    , s.manager_id
    , s.manager_name
    , s.store_id

    , so.total_orders
    , so.total_revenue
    , so.avg_processing_time

    -- Revenu moyen par commande pour mesurer la qualité de la vente (upselling)
    , ROUND(SAFE_DIVIDE(so.total_revenue, so.total_orders),2) AS avg_order_value
FROM staff s
LEFT JOIN staff_orders so ON s.staff_id = so.staff_id