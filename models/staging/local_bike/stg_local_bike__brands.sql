WITH brands AS (
    SELECT
    *
    FROM {{ source('local_bike_dataset', 'brands') }}
)

SELECT 
    brand_id
    , brand_name
    FROM brands