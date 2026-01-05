WITH orders AS (
    SELECT 
        order_id
        , customer_id
        , store_id
        , staff_id
        , order_status
        , order_date
        , required_date
        , shipped_date
    FROM {{ ref('stg_local_bike__orders') }}
)

, order_items AS (
    SELECT
        order_id
        , product_id
        , quantity
        , unit_price
        , discount
    FROM {{ ref('stg_local_bike__order_items') }}
)

, products_enriched AS (
    SELECT
        product_id
        , product_name
        , category_name
        , brand_name
    FROM {{ ref('int_local_bike__dim_items') }}
)

, orders_cleaned AS (
    SELECT 
        o.order_id
        , o.customer_id
        , o.store_id
        , o.staff_id
        , o.order_status
        , o.order_date
        , o.required_date
        , o.shipped_date
        , oi.product_id
        , pe.product_name
        , pe.category_name
        , pe.brand_name

        , ROUND(SUM((oi.quantity * oi.unit_price)),2) AS total_order_amount
        , ROUND(SUM((oi.quantity * oi.unit_price) * (1 - oi.discount)),2) AS total_order_amount_net
        , COUNT(oi.product_id) AS total_items_count
    FROM orders o
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN products_enriched pe ON pe.product_id = oi.product_id
    GROUP BY ALL
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