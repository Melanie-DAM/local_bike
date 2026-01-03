WITH stocks AS (
    SELECT
    *
    FROM {{ source('local_bike_dataset', 'stocks') }}
)

SELECT
    SAFE_CAST(store_id AS INT64) AS store_id
    , SAFE_CAST(product_id AS INT64) AS product_id
    , SAFE_CAST(quantity AS INT64) AS quantity
FROM stocks