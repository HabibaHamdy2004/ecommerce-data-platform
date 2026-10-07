with dates as (

    select distinct
        cast(order_purchase_timestamp as date) as date_day
    from {{ ref('stg_orders') }}
    where order_purchase_timestamp is not null

)

select
    date_day,
    year(date_day) as year_number,
    month(date_day) as month_number,
    monthname(date_day) as month_name,
    day(date_day) as day_number,
    dayofweek(date_day) as day_of_week
from dates