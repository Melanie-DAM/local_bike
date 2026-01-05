WITH order_items AS (
    SELECT
        product_id
        , unit_price -- Le prix catalogue au moment de la vente
        , discount
        , quantity

        -- Revenu Brut vs Revenu Net
        , (quantity * unit_price) AS total_gross_amount
        , (quantity * unit_price) * (1 - discount) AS total_net_amount

        -- Montant de la remise accordée
        , (quantity * unit_price) * discount AS discount_amount

        -- Identification des ventes avec vs sans remise
        , CASE WHEN discount > 0 THEN TRUE ELSE FALSE END AS is_discounted
    FROM {{ ref('stg_local_bike__order_items')}}
)

, products AS (
    SELECT
        product_id
        , product_name
        , brand_id
        , category_id
    FROM {{ ref('stg_local_bike__products') }}
)

, products_perf AS (
    SELECT
        oi.product_id
        , p.product_name
        , p.brand_id
        , p.category_id
        , oi.unit_price
        , oi.discount
        , oi.quantity
        
        , oi.total_gross_amount
        , oi.total_net_amount
        
        , oi.discount_amount
        , oi.is_discounted
    FROM order_items oi
    LEFT JOIN products p ON oi.product_id = p.product_id
)

SELECT *
FROM products_perf