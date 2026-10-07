with customer_orders as (

    select
        order_id,
        customer_id,
        cast(order_purchase_timestamp as date) as order_date

    from {{ ref('fact_order') }}

),

order_sales as (

    select
        order_id,

        sum(price) as product_revenue,
        sum(freight_value) as freight_revenue,
        sum(price + freight_value) as total_revenue

    from {{ ref('fact_sales') }}

    group by order_id

)

select

    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,

    count(distinct co.order_id) as order_count,

    coalesce(sum(os.product_revenue), 0) as product_revenue,

    coalesce(sum(os.freight_revenue), 0) as freight_revenue,

    coalesce(sum(os.total_revenue), 0) as total_spend,

    coalesce(avg(os.total_revenue), 0) as avg_order_value,

    min(co.order_date) as first_order_date,

    max(co.order_date) as last_order_date,

    case
        when count(distinct co.order_id) > 1
        then 'Repeat'
        else 'One-time'
    end as customer_type

from customer_orders co

left join order_sales os
    on co.order_id = os.order_id

left join {{ ref('dim_customer') }} c
    on co.customer_id = c.customer_id

group by

    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state