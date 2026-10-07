select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    shipping_limit_date
from {{ source('olist', 'order_items') }}