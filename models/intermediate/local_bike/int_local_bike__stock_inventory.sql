WITH stocks AS (
    SELECT
    *
    FROM {{ ref ('stg_local_bike__stocks')}}
)

, products AS (
    SELECT
    *
    FROM {{ ref ('stg_local_bike__products')}}
)

, categories AS (
    SELECT
    *
    FROM {{ ref ('stg_local_bike__categories')}}
)

, brands AS (
    SELECT
    *
    FROM {{ ref ('stg_local_bike__brands')}}
)

SELECT
    s.store_id
    , p.product_id
    , p.product_name
    , c.category_name
    , b.brand_name
    , s.quantity AS current_stock
    , CASE 
        WHEN s.quantity = 0 THEN 'Out of stock'
        WHEN s.quantity <10 THEN 'Low stock'
        ELSE 'Good stock'
    END AS stock_status

FROM stocks s
LEFT JOIN products p ON p.product_id = s.product_id
LEFT JOIN categories c ON c.category_id = p.category_id
LEFT JOIN brands b ON b.brand_id = p.brand_id