-- =============================================================================
-- HERMES Operational Storage Schema (PostgreSQL 16)
-- Module: MOD-05 (MOD-05-FN-01)
-- Owner: Kushagra
-- Authority: Lab 6-7-8 Report Section 6 & Blueprint Section 11.4
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------------------------------------------------------
-- 1. Customers Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id UUID PRIMARY KEY,
    full_name VARCHAR(120) NOT NULL,
    email VARCHAR(160) NOT NULL,
    home_country CHAR(2) NOT NULL,
    home_city VARCHAR(100) NOT NULL,
    home_latitude DOUBLE PRECISION NOT NULL,
    home_longitude DOUBLE PRECISION NOT NULL,
    risk_tier VARCHAR(20) NOT NULL DEFAULT 'low' CHECK (risk_tier IN ('low', 'medium', 'high', 'critical')),
    avg_transaction_amount NUMERIC(12, 2) NOT NULL DEFAULT 75.00,
    card_type VARCHAR(20) NOT NULL DEFAULT 'credit',
    account_created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- 2. Merchants Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS merchants (
    merchant_id UUID PRIMARY KEY,
    name VARCHAR(160) NOT NULL,
    category VARCHAR(64) NOT NULL,
    country CHAR(2) NOT NULL,
    city VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    is_high_risk BOOLEAN NOT NULL DEFAULT FALSE,
    is_blacklisted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- 3. Transactions Table (Operational & Audit Provenance Store)
-- Primary Key on event_id enforces unique B-Tree index for idempotent upserts:
-- ON CONFLICT (event_id) DO NOTHING / DO UPDATE
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
    event_id UUID PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    customer_id UUID NOT NULL,
    merchant_id UUID NOT NULL,
    amount NUMERIC(14, 2) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    transaction_type VARCHAR(32) NOT NULL DEFAULT 'purchase',
    channel VARCHAR(32) NOT NULL DEFAULT 'online',
    card_type VARCHAR(20) NOT NULL DEFAULT 'credit',
    category VARCHAR(64) NOT NULL,
    country CHAR(2) NOT NULL,
    city VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    device_id VARCHAR(64),
    device_type VARCHAR(32),
    device_os VARCHAR(64),
    ip_address VARCHAR(45),
    is_international BOOLEAN NOT NULL DEFAULT FALSE,
    status VARCHAR(32) NOT NULL DEFAULT 'completed',

    -- Fraud Decision & Scoring Columns (Lab 6-7-8 Section 4 & 5)
    fraud_score DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (fraud_score >= 0.0 AND fraud_score <= 1.0),
    rule_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    ml_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    stat_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    z_score_signal DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    risk_band VARCHAR(16) NOT NULL DEFAULT 'low' CHECK (risk_band IN ('low', 'medium', 'high', 'critical')),
    decision VARCHAR(16) NOT NULL DEFAULT 'approve' CHECK (decision IN ('approve', 'flag', 'block')),
    triggered_rules JSONB NOT NULL DEFAULT '[]'::jsonb,
    reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    feature_values JSONB NOT NULL DEFAULT '{}'::jsonb,
    model_version VARCHAR(64) NOT NULL DEFAULT 'ensemble-v1.0.0',
    shadow_model_version VARCHAR(64),
    shadow_ml_score DOUBLE PRECISION,
    rule_version VARCHAR(64) NOT NULL DEFAULT 'rules-v1.0.0',
    transformation_version VARCHAR(64) NOT NULL DEFAULT 'features-v1.0.0',
    feature_time TIMESTAMPTZ,
    decision_time TIMESTAMPTZ,
    processing_time_ms INTEGER DEFAULT 0,

    -- Audit & Provenance Columns (Lab 6-7-8 Section 5 & 6, NFR-05)
    source_topic VARCHAR(128) NOT NULL DEFAULT 'hermes.transactions.raw',
    kafka_partition INTEGER DEFAULT 0,
    kafka_offset BIGINT DEFAULT 0,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    storage_destinations JSONB NOT NULL DEFAULT '["postgres:transactions"]'::jsonb,
    schema_version VARCHAR(16) NOT NULL DEFAULT '1.0'
);

-- -----------------------------------------------------------------------------
-- 4. Fraud Alerts Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id VARCHAR(80) PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE REFERENCES transactions(event_id) ON DELETE CASCADE,
    event_time TIMESTAMPTZ NOT NULL,
    customer_id UUID NOT NULL,
    merchant_id UUID NOT NULL,
    amount NUMERIC(14, 2) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    category VARCHAR(64),
    fraud_score DOUBLE PRECISION NOT NULL CHECK (fraud_score >= 0.0 AND fraud_score <= 1.0),
    risk_band VARCHAR(16) NOT NULL CHECK (risk_band IN ('low', 'medium', 'high', 'critical')),
    decision VARCHAR(16) NOT NULL CHECK (decision IN ('approve', 'flag', 'block')),
    triggered_rules JSONB NOT NULL DEFAULT '[]'::jsonb,
    status VARCHAR(24) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'acknowledged', 'dismissed')),
    analyst_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- 5. Mandatory Workload-Aligned Indexes (Lab 6-7-8 Section 6)
-- -----------------------------------------------------------------------------

-- Customer history & velocity drill-down
CREATE INDEX IF NOT EXISTS idx_transactions_customer_time
    ON transactions (customer_id, event_time DESC);

-- Merchant investigation & risk review
CREATE INDEX IF NOT EXISTS idx_transactions_merchant_time
    ON transactions (merchant_id, event_time DESC);

-- Dashboard risk band filtering & anomaly scatter timeline
CREATE INDEX IF NOT EXISTS idx_transactions_risk_time
    ON transactions (risk_band, event_time DESC);

-- Merchant category breakdown queries
CREATE INDEX IF NOT EXISTS idx_transactions_category_time
    ON transactions (category, event_time DESC);

-- JSONB rule trigger frequency analytics
CREATE INDEX IF NOT EXISTS idx_transactions_triggered_rules_gin
    ON transactions USING GIN (triggered_rules);

-- Active alert queue & state management
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_status_time
    ON fraud_alerts (status, event_time DESC);
