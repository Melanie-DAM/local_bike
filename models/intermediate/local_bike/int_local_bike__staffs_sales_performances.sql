WITH staff_orders AS (
    -- On agrège les performances de ventes par staff
    SELECT 
        staff_id,
        store_id,
        COUNT(order_id) AS total_orders,
        SUM(total_order_amount) AS total_revenue,
        AVG(lead_time_days) AS avg_processing_time
    FROM {{ ref('int_local_bike__orders_performances') }}
    WHERE order_status = 4 -- On ne compte que les ventes complétées
    GROUP BY 1, 2
)

SELECT
    s.staff_id,
    s.staff_name,
    s.manager_id,
    m.staff_name AS manager_name, -- Jointure pour avoir le nom du manager
    s.store_id,
    so.total_orders,
    so.total_revenue,
    so.avg_processing_time,
    -- Revenu moyen par commande pour mesurer la qualité de la vente (upselling)
    SAFE_DIVIDE(so.total_revenue, so.total_orders) AS avg_order_value
FROM {{ ref('stg_local_bike__staffs') }} s
LEFT JOIN staff_orders so ON s.staff_id = so.staff_id
LEFT JOIN {{ ref('stg_local_bike__staffs') }} m ON s.manager_id = m.staff_id -- Self-join pour la hiérarchie