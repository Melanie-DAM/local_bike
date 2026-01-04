WITH customers AS (
    SELECT 
    *
    FROM {{ source('local_bike_dataset', 'customers') }}
)

SELECT
    CAST(customer_id AS INT64) AS customer_id
    , CONCAT(UPPER(last_name), ' ', first_name) AS customer_name
    , city AS customer_city
    , state AS customer_state
    , CAST(zip_code AS STRING) AS customer_zip_code
FROM customers