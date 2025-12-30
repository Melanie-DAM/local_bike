SELECT
    store_id
    , store_name
    , city AS store_city
    , state AS store_state
    , zip_code AS store_zip_code
    , street AS store_street
from {{ source('local_bike_dataset', 'stores') }}