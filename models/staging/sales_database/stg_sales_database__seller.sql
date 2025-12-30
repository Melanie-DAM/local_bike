select seller_id,
 seller_zip_code,
 seller_city,
 seller_state
from {{ source('databird_ae_sales_data', 'seller') }}