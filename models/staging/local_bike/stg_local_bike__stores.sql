WITH stores AS (
    SELECT
    *
    FROM {{ source('local_bike_dataset', 'stores') }}
)

SELECT
    SAFE_CAST(store_id AS INT64) AS store_id
    , TRIM(store_name) AS store_name
    , UPPER(TRIM(city)) AS store_city
    , UPPER(TRIM(state)) AS store_state
    , CAST(zip_code AS STRING) AS store_zip_code
    , TRIM(street) AS store_street
FROM stores