-- =============================================================================
-- HERMES TimescaleDB Time-Series Schema, Hypertables & Continuous Aggregates
-- Module: MOD-05 (MOD-05-FN-02)
-- Owner: Kushagra
-- Authority: Lab 6-7-8 Report Section 6 & Blueprint Section 11.4
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- -----------------------------------------------------------------------------
-- 1. Hypertable: transaction_metrics (System & Window Rollups)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transaction_metrics (
    bucket_time TIMESTAMPTZ NOT NULL,
    window_size VARCHAR(16) NOT NULL DEFAULT '1m',
    channel VARCHAR(32) NOT NULL DEFAULT 'all',
    region VARCHAR(64) NOT NULL DEFAULT 'all',
    tx_count BIGINT NOT NULL DEFAULT 0,
    tps DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    total_amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    avg_amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    max_amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    p95_amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    fraud_count BIGINT NOT NULL DEFAULT 0,
    flagged_count BIGINT NOT NULL DEFAULT 0,
    blocked_count BIGINT NOT NULL DEFAULT 0,
    fraud_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    p95_latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    unique_customers BIGINT NOT NULL DEFAULT 0,
    unique_merchants BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_time, window_size, channel, region)
);

SELECT create_hypertable(
    'transaction_metrics',
    'bucket_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_tx_metrics_window_bucket
    ON transaction_metrics (window_size, bucket_time DESC);

-- -----------------------------------------------------------------------------
-- 2. Hypertable: category_metrics (Merchant Category Time-Series Rollups)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS category_metrics (
    bucket_time TIMESTAMPTZ NOT NULL,
    category VARCHAR(64) NOT NULL,
    tx_count BIGINT NOT NULL DEFAULT 0,
    total_amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    fraud_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_time, category)
);

SELECT create_hypertable(
    'category_metrics',
    'bucket_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- -----------------------------------------------------------------------------
-- 3. Hypertable: geo_metrics (Geospatial Time-Series Rollups)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS geo_metrics (
    bucket_time TIMESTAMPTZ NOT NULL,
    country CHAR(2) NOT NULL,
    city VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    tx_count BIGINT NOT NULL DEFAULT 0,
    total_amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    fraud_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_time, country, city)
);

SELECT create_hypertable(
    'geo_metrics',
    'bucket_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- -----------------------------------------------------------------------------
-- 4. Continuous Aggregates: cagg_tx_metrics_5m & cagg_tx_metrics_1h
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_tx_metrics_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', bucket_time) AS bucket_5m,
    window_size,
    SUM(tx_count) AS tx_count,
    AVG(tps) AS avg_tps,
    SUM(total_amount) AS total_amount,
    AVG(avg_amount) AS avg_amount,
    MAX(max_amount) AS max_amount,
    MAX(p95_amount) AS p95_amount,
    SUM(fraud_count) AS fraud_count,
    SUM(flagged_count) AS flagged_count,
    SUM(blocked_count) AS blocked_count,
    AVG(p95_latency_ms) AS p95_latency_ms,
    MAX(unique_customers) AS unique_customers,
    MAX(unique_merchants) AS unique_merchants
FROM transaction_metrics
GROUP BY bucket_5m, window_size
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'cagg_tx_metrics_5m',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_tx_metrics_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket_time) AS bucket_1h,
    window_size,
    SUM(tx_count) AS tx_count,
    AVG(tps) AS avg_tps,
    SUM(total_amount) AS total_amount,
    AVG(avg_amount) AS avg_amount,
    MAX(max_amount) AS max_amount,
    MAX(p95_amount) AS p95_amount,
    SUM(fraud_count) AS fraud_count,
    SUM(flagged_count) AS flagged_count,
    SUM(blocked_count) AS blocked_count,
    AVG(p95_latency_ms) AS p95_latency_ms,
    MAX(unique_customers) AS unique_customers,
    MAX(unique_merchants) AS unique_merchants
FROM transaction_metrics
GROUP BY bucket_1h, window_size
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'cagg_tx_metrics_1h',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- -----------------------------------------------------------------------------
-- 5. Native Columnar Compression (7 Days) & Retention Policies (90 Days)
-- -----------------------------------------------------------------------------
ALTER TABLE transaction_metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'window_size, channel, region',
    timescaledb.compress_orderby = 'bucket_time DESC'
);

ALTER TABLE category_metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'category',
    timescaledb.compress_orderby = 'bucket_time DESC'
);

ALTER TABLE geo_metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'country, city',
    timescaledb.compress_orderby = 'bucket_time DESC'
);

SELECT add_compression_policy('transaction_metrics', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('category_metrics', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('geo_metrics', INTERVAL '7 days', if_not_exists => TRUE);

SELECT add_retention_policy('transaction_metrics', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('category_metrics', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('geo_metrics', INTERVAL '90 days', if_not_exists => TRUE);
