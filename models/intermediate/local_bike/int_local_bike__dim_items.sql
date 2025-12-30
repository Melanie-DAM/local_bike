WITH  products AS (
    SELECT 
    *
    FROM {{ ref ('stg_local_bike__products') }}
)


, categ AS (
    SELECT 
    *
    FROM {{ ref ('stg_local_bike__categories') }}
)


, brands AS (
    SELECT 
    *
    FROM {{ ref ('stg_local_bike__brands') }}
)

SELECT 
    p.product_id
    , p.product_name
    , p.model_year
    , p.unit_price
    , p.category_id
    , c.category_name
    , p.brand_id
    , b.brand_name

FROM products p 
LEFT JOIN categ c ON c.category_id = p.category_id
LEFT JOIN brands b ON b.brand_id = p.brand_id
