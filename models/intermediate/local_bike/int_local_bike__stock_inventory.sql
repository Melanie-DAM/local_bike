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

, stores AS (
    SELECT
    *
    FROM {{ ref ('stg_local_bike__stores')}}
)


SELECT
    s.store_id
    , st.store_name
    , st.store_city
    , st.store_state
    , st.store_zip_code
    , st.store_street
    , p.product_id
    , p.product_name
    , s.quantity AS current_stock
    , CASE 
        WHEN s.quantity = 0 THEN 'Out of stock'
        WHEN s.quantity <10 THEN 'Low stock'
        ELSE 'Good stock'
    END AS stock_status

FROM stocks s
LEFT JOIN products p ON p.product_id = s.product_id
LEFT JOIN stores st ON st.store_id = s.store_id