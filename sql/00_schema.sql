-- Runs once, when the warehouse Postgres volume is first created.
-- Source data lives in schema "src"; ingestion lands it in schema "raw";
-- dbt builds "analytics" on top of raw.

CREATE SCHEMA IF NOT EXISTS src;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Shape shared by every source table (orders_1m, orders_10m, ...).
CREATE TABLE IF NOT EXISTS src.orders_template (
    order_id     bigint        NOT NULL,
    customer_id  integer       NOT NULL,
    order_ts     timestamptz   NOT NULL,
    status       text          NOT NULL,
    amount       numeric(12,2) NOT NULL,
    currency     char(3)       NOT NULL,
    country      text          NOT NULL,
    note         text
);

-- Landing table shape: the source columns plus the ingestion batch id.
-- Every ingestion target (raw.orders, raw.bench_<lang>) is created LIKE this.
CREATE TABLE IF NOT EXISTS raw.orders_template (
    LIKE src.orders_template,
    batch_id text NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.orders (LIKE raw.orders_template);
CREATE TABLE IF NOT EXISTS raw.bench_python (LIKE raw.orders_template);
CREATE TABLE IF NOT EXISTS raw.bench_go (LIKE raw.orders_template);
CREATE TABLE IF NOT EXISTS raw.bench_java (LIKE raw.orders_template);
CREATE TABLE IF NOT EXISTS raw.bench_ts (LIKE raw.orders_template);
