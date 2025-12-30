SELECT
    customer_id
    , CONCAT(UPPER(last_name), ' ', first_name) AS customer_name
    , city AS customer_city
    , state AS customer_state
    , zip_code AS customer_zip_code
from {{ source('local_bike_dataset', 'customers') }}