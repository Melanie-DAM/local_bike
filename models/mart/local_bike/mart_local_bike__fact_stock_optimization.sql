{{ config(materialized='table') }}

WITH stock_velocity AS (
    SELECT
    *
    FROM {{ ref('int_local_bike__stock_velocity') }}
)

, stores AS (
    SELECT 
    *
    FROM {{ ref('stg_local_bike__stores') }}
)

, stock_optim AS (
    SELECT
        sv.*,
        st.store_name,
        -- Alerte de rupture (Moins de 7 jours de stock)
        CASE 
            WHEN sv.days_of_stock_remaining < 7 THEN 'CRITICAL: Reorder'
            WHEN sv.days_of_stock_remaining BETWEEN 7 AND 21 THEN 'WARNING: Low Stock'
            WHEN sv.days_of_stock_remaining > 90 THEN 'SURPLUS: Overstock'
            ELSE 'Healthy'
        END AS inventory_health_status,

        -- Optimisation inter-magasins : Stock total du même produit ailleurs
        SUM(sv.current_stock_level) OVER(PARTITION BY sv.product_id) - sv.current_stock_level AS available_in_other_stores,

        -- Taux de rotation simplifié (Ratio vente/stock)
        SAFE_DIVIDE(sv.daily_sales_velocity * 30, sv.current_stock_level) AS monthly_turnover_rate
    FROM stock_velocity sv
    LEFT JOIN stores st ON sv.store_id = st.store_id
)

SELECT *
FROM stock_optim