WITH staff_sales_perf AS (
    SELECT
        staff_id
        , staff_name
        , manager_id
        , manager_name
        , store_id
        , order_date
        , total_orders
        , total_revenue
        , total_revenue_net
        , avg_processing_time
        , avg_order_value
    FROM {{ ref('int_local_bike__staffs_sales_performances') }}
)

    -- Calcul de la charge de travail globale du magasin
, store_stats AS (
    SELECT 
        store_id
        , COUNT(DISTINCT staff_id) AS staff_count
        , SUM(total_orders) AS store_total_orders
    FROM staff_sales_perf
    GROUP BY 1
)


, staff_sales_enriched AS (
    SELECT
        ssp.staff_id
        , ssp.staff_name
        , ssp.manager_id
        , ssp.manager_name
        , ssp.store_id
        , st.store_name
        , ssp.order_date
        , ssp.total_orders
        , ssp.total_revenue
        , ssp.total_revenue_net
        , ssp.avg_processing_time
        , ssp.avg_order_value
        -- Ratio de contribution du staff au volume du magasin
        , SAFE_DIVIDE(ssp.total_orders, ss.store_total_orders) * 100 AS pct_contribution_to_store
        -- Charge de travail moyenne du magasin (Orders per staff)
        , SAFE_DIVIDE(ss.store_total_orders, ss.staff_count) AS store_workload_index
    FROM staff_sales_perf ssp
    LEFT JOIN store_stats ss ON ssp.store_id = ss.store_id
    LEFT JOIN {{ ref('stg_local_bike__stores') }} st ON ssp.store_id = st.store_id
)

SELECT *
FROM staff_sales_enriched