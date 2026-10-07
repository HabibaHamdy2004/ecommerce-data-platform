select

    o.order_id,
    o.customer_id,
    o.order_status,

    cast(o.order_purchase_timestamp as date) as purchase_date,
    cast(o.order_approved_at as date) as approved_date,
    cast(o.order_delivered_carrier_date as date) as delivered_carrier_date,
    cast(o.order_delivered_customer_date as date) as delivered_date,
    cast(o.order_estimated_delivery_date as date) as estimated_delivery_date,

    datediff(
        'day',
        cast(o.order_purchase_timestamp as date),
        cast(o.order_delivered_customer_date as date)
    ) as delivery_days,

    datediff(
        'day',
        cast(o.order_purchase_timestamp as date),
        cast(o.order_estimated_delivery_date as date)
    ) as estimated_delivery_days,

    case
        when o.order_delivered_customer_date is not null
         and o.order_estimated_delivery_date is not null
         and o.order_delivered_customer_date > o.order_estimated_delivery_date
        then 1
        else 0
    end as is_late

from {{ ref('fact_order') }} o