SELECT
    order_id
    , item_id
    , product_id
    , quantity
    , list_price AS unit_price
    , discount
    , ROUND((quantity * list_price) , 2) AS order_item_revenue
    , ROUND((quantity * list_price) * (1 - discount), 2) AS order_item_net_revenue
from {{ source('local_bike_dataset', 'order_items') }}