WITH products AS (
    SELECT 
    *
    FROM {{ source('local_bike_dataset', 'products') }}
)

SELECT
    SAFE_CAST(product_id AS INT64) AS product_id
    , TRIM(product_name) AS product_name
    , SAFE_CAST(brand_id AS INT64) AS brand_id
    , SAFE_CAST(category_id AS INT64) AS category_id
    , SAFE_CAST(model_year AS INT64) AS model_year
    , SAFE_CAST(list_price AS NUMERIC) AS unit_price
FROM products