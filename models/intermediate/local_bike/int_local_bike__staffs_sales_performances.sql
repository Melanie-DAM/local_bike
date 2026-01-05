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
        , SUM(total_order_amount) AS total_revenue
        , SUM(total_order_amount_net) AS total_revenue_net
        , AVG(processing_time_days) AS avg_processing_time
    FROM {{ ref('int_local_bike__orders_performances') }}
    GROUP BY 1, 2, 3
)

, staff_orders_enriched AS (
    SELECT
        s.staff_id
        , s.staff_name
        , s.manager_id
        , s.manager_name
        , s.store_id
        , so.order_date

        , so.total_orders
        , so.total_revenue
        , so.total_revenue_net
        , so.avg_processing_time

        -- Revenu moyen par commande pour mesurer la qualité de la vente (upselling)
        , SAFE_DIVIDE(so.total_revenue, so.total_orders) AS avg_order_value

        , so.total_orders
        , so.total_revenue
        , so.avg_processing_time
    FROM staff s
    LEFT JOIN staff_orders so ON s.staff_id = so.staff_id
)

SELECT *
FROM staff_orders_enriched