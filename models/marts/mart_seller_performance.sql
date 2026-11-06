select

    s.seller_id,

    d.seller_city,
    d.seller_state,

    count(distinct s.order_id) as order_count,

    count(*) as items_sold,

    sum(s.price) as product_revenue,

    sum(s.freight_value) as freight_revenue,

    sum(s.price + s.freight_value) as total_revenue,

    avg(s.price) as avg_item_price,

    avg(s.freight_value) as avg_freight

from {{ ref('fact_sales') }} s

left join {{ ref('dim_seller') }} d
    on s.seller_id = d.seller_id

group by

    s.seller_id,
    d.seller_city,
    d.seller_state