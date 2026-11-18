-- Daily order count and revenue per country, status and currency.
select
    batch_id,
    order_date,
    country,
    status,
    currency,
    count(*)            as order_count,
    sum(amount)         as revenue
from {{ ref('stg_orders') }}
group by 1, 2, 3, 4, 5
