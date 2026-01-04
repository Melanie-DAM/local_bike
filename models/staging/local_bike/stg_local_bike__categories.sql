WITH categories AS (
    SELECT
    *
    FROM {{ source('local_bike_dataset', 'categories') }}
)

SELECT 
    category_id
    , category_name
    FROM categories