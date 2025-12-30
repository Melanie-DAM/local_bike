SELECT
    *
from {{ source('local_bike_dataset', 'categories') }}