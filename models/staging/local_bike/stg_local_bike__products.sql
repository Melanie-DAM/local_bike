SELECT
    product_id
    , product_name
    , list_price AS unit_price
    , model_year
    , brand_id
    , category_id
from {{ source('local_bike_dataset', 'products') }}