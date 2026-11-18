-- Deterministic synthetic orders. Usage (psql variables):
--   psql -v rows=1000000 -v tbl=orders_1m -f seed.sql
-- Re-running replaces the table. setseed() makes random() repeatable, so every
-- run of the benchmark reads byte-identical source data.

\set ON_ERROR_STOP on
DROP TABLE IF EXISTS src.:tbl;
CREATE TABLE src.:tbl (LIKE src.orders_template);

SELECT setseed(0.42);

INSERT INTO src.:tbl
SELECT
    g                                                         AS order_id,
    1 + (random() * 199999)::int                              AS customer_id,
    timestamptz '2026-01-01 00:00:00+00'
        + (random() * 300 * 86400) * interval '1 second'      AS order_ts,
    (ARRAY['placed','paid','shipped','delivered','returned','cancelled'])
        [1 + (random() * 5)::int]                             AS status,
    round((1 + random() * 999)::numeric, 2)                   AS amount,
    (ARRAY['EUR','USD','GBP','ARS'])[1 + (random() * 3)::int] AS currency,
    (ARRAY['AR','BR','DE','ES','FR','GB','NL','US'])[1 + (random() * 7)::int] AS country,
    CASE WHEN random() < 0.2 THEN NULL
         ELSE 'gift wrap: ' || md5(g::text) END               AS note
FROM generate_series(1, :rows) AS g;

ALTER TABLE src.:tbl ADD PRIMARY KEY (order_id);
ANALYZE src.:tbl;
SELECT :'tbl' AS seeded_table, count(*) AS row_count, pg_size_pretty(pg_total_relation_size('src.' || :'tbl')) AS size
FROM src.:tbl;
