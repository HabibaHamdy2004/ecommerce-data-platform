with sales as (

    select
        order_id,
        price,
        freight_value
    from {{ ref('fact_sales') }}

),

orders as (

    select
        order_id,
        cast(order_purchase_timestamp as date) as order_date,
        order_status
    from {{ ref('fact_order') }}

),

monthly_sales as (

    select

        year(o.order_date) as year_number,
        month(o.order_date) as month_number,
        monthname(o.order_date) as month_name,

        o.order_status,

        count(distinct o.order_id) as order_count,

        count(s.order_id) as item_count,

        sum(s.price) as product_revenue,

        sum(s.freight_value) as freight_revenue,

        sum(s.price + s.freight_value) as total_revenue,

        sum(s.price + s.freight_value)
            / nullif(count(distinct o.order_id), 0)
            as average_order_value,

        avg(s.price + s.freight_value)
            as average_item_value

    from orders o

    left join sales s
        on o.order_id = s.order_id

    where o.order_date is not null

    group by

        year(o.order_date),
        month(o.order_date),
        monthname(o.order_date),
        o.order_status

) 

select *

from monthly_sales

order by
    year_number,
    month_number,
    order_status