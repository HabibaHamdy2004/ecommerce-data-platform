select

    p.payment_type,
    p.payment_installments,

    count(*) as payment_count,

    count(distinct p.order_id) as order_count,

    sum(p.payment_value) as total_payment_value,

    avg(p.payment_value) as average_payment_value

from {{ ref('fact_payment') }} p

group by

    p.payment_type,
    p.payment_installments