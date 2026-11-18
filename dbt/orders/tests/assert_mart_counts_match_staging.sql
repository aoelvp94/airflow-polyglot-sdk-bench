-- Fails (returns a row) if the mart lost or duplicated orders.
{{ config(tags=["orders"]) }}
with s as (select count(*) as n from {{ ref('stg_orders') }}),
     m as (select coalesce(sum(order_count), 0) as n from {{ ref('fct_orders_daily') }})
select s.n as staging_rows, m.n as mart_rows
from s cross join m
where s.n <> m.n
