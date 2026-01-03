WITH order_items AS (
    SELECT
    *
    FROM {{ source('local_bike_dataset', 'order_items') }}
)

SELECT
    SAFE_CAST(order_id AS INT64) AS order_id
    , SAFE_CAST(item_id AS INT64) AS item_id
    , SAFE_CAST(product_id AS INT64) AS product_id
    , CAST(quantity AS INT64) AS quantity

    , CAST(list_price AS NUMERIC) AS unit_price
    , SAFE_CAST(discount AS NUMERIC) AS discount
FROM order_items