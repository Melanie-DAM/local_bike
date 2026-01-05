WITH stocks AS (
    SELECT
        store_id
        , product_id
        , quantity
    FROM {{ ref('stg_local_bike__stocks') }}
)

, products AS (
    SELECT
        product_id
        , product_name
        , brand_id
    FROM {{ ref('stg_local_bike__products') }}
)

, order_items AS (
    SELECT 
        product_id
        , order_id
        , quantity
    FROM {{ ref('stg_local_bike__order_items') }}
)

, orders AS (
    SELECT
        order_id
        , order_date
        , store_id
    FROM {{ ref('stg_local_bike__orders') }}
)

, sales_velocity AS (
    -- ventes quotidiennes moyennes sur les 90 derniers jours
    SELECT 
        o.store_id
        , oi.product_id
        , SUM(oi.quantity) AS total_units_sold
        -- Vitesse de vente (unités/jour)
        , SAFE_DIVIDE(SUM(oi.quantity), 90) AS daily_sales_velocity
    FROM order_items oi
    LEFT JOIN orders o ON oi.order_id = o.order_id
    GROUP BY 1, 2
)



, sales_velocity_enriched AS (
    SELECT
        s.store_id
        , s.product_id
        , p.product_name
        , p.brand_id
        , s.quantity AS current_stock_level
        , CASE 
            WHEN s.quantity = 0 THEN 'Out of stock'
            WHEN s.quantity <10 THEN 'Low stock'
            ELSE 'Good stock'
        END AS stock_status
        , v.total_units_sold
        , v.daily_sales_velocity

        -- Autonomie du stock (en jours)
        , CASE 
            WHEN v.daily_sales_velocity > 0 THEN SAFE_DIVIDE(s.quantity, v.daily_sales_velocity)
            WHEN s.quantity > 0 THEN 999 -- (pas de ventes)
            ELSE 0 
        END AS days_of_stock_remaining
    FROM stocks s
    LEFT JOIN sales_velocity v ON s.store_id = v.store_id AND s.product_id = v.product_id
    LEFT JOIN products p ON s.product_id = p.product_id
)

SELECT *
FROM sales_velocity_enriched