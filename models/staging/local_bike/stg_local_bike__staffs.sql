SELECT
    staff_id
    , CONCAT(UPPER(last_name), ' ', first_name) AS staff_name
    , store_id
    , manager_id
from {{ source('local_bike_dataset', 'staffs') }}