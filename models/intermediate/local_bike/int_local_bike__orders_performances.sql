WITH orders_cleaned AS (
    SELECT 
        o.order_id
        , o.customer_id
        , o.store_id
        , o.staff_id
        , o.order_status
        , o.order_date
        , o.required_date
        , o.shipped_date
        
        , ROUND(SUM((oi.quantity * oi.unit_price)),2) AS total_order_amount
        , ROUND(SUM((oi.quantity * oi.unit_price) * (1 - oi.discount)),2) AS total_order_amount_net
        , COUNT(oi.product_id) AS total_items_count

    FROM {{ ref('stg_local_bike__orders') }} o
    LEFT JOIN {{ ref('stg_local_bike__order_items') }} oi ON o.order_id = oi.order_id
    GROUP BY 1,2,3,4,5,6,7,8
)

SELECT
    *
    -- Délai de traitement (en jours)
    , DATE_DIFF(shipped_date, order_date, DAY) AS processing_time_days
    
    -- Fiabilité : Écart par rapport à la promesse
    , DATE_DIFF(shipped_date, required_date, DAY) AS days_diff_from_required
    
    -- Flag de retard
    , CASE 
        WHEN shipped_date > required_date THEN 1 
        ELSE 0 
      END AS is_late_shipping
FROM orders_cleaned