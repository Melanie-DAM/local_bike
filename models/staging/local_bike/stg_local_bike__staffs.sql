WITH staffs AS (
    SELECT
    *
    FROM {{ source('local_bike_dataset', 'staffs') }}
)

SELECT
    SAFE_CAST(staff_id AS INT64) AS staff_id
    , CONCAT(UPPER(TRIM(last_name)), ' ', TRIM(first_name)) AS staff_name
    
    , SAFE_CAST(store_id AS INT64) AS store_id
    , SAFE_CAST(manager_id AS INT64) AS manager_id
FROM staffs