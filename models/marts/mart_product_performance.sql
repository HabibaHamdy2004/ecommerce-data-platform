select

    p.product_id,
    p.product_category_name,

    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,

    count(*) as units_sold,

    count(distinct s.order_id) as order_count,

    sum(s.price) as product_revenue,

    sum(s.freight_value) as freight_revenue,

    sum(s.price + s.freight_value) as total_revenue,

    avg(s.price) as avg_item_price,

    avg(s.freight_value) as avg_freight

from {{ ref('fact_sales') }} s

left join {{ ref('dim_product') }} p
    on s.product_id = p.product_id

group by

    p.product_id,
    p.product_category_name,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm