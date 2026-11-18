-- One row per order of the batch being processed (or every batch when no
-- batch_id var is given).
select
    order_id,
    customer_id,
    order_ts,
    order_ts::date                as order_date,
    status,
    amount,
    upper(currency)               as currency,
    country,
    note,
    batch_id
from {{ source('raw', 'orders') }}
{% if var('batch_id') is not none %}
where batch_id = '{{ var("batch_id") }}'
{% endif %}
