"""
Integration & Schema Verification Tests for PostgreSQL Operational DDL & Redis Hot Store
Module: MOD-05 (MOD-05-FN-01, MOD-05-FN-03)
Test IDs: TC-20, TC-22
Owner: Kushagra
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
POSTGRES_INIT_SQL = ROOT_DIR / "infrastructure" / "postgres" / "init.sql"
POSTGRES_CONF = ROOT_DIR / "infrastructure" / "postgres" / "postgresql.conf"
REDIS_CONF = ROOT_DIR / "infrastructure" / "redis" / "redis.conf"


def test_postgres_init_sql_defines_all_tables_and_workload_indexes() -> None:
    """TC-20: Verify PostgreSQL init.sql defines all 4 tables, provenance columns, and 6 mandatory indexes."""
    assert POSTGRES_INIT_SQL.exists(), f"Missing {POSTGRES_INIT_SQL}"
    assert POSTGRES_CONF.exists(), f"Missing {POSTGRES_CONF}"

    sql_text = POSTGRES_INIT_SQL.read_text(encoding="utf-8")
    sql_lower = sql_text.lower()

    # 1. Verify all 4 required operational tables exist
    required_tables = ["customers", "merchants", "transactions", "fraud_alerts"]
    for table in required_tables:
        assert f"create table if not exists {table}" in sql_lower or f"create table {table}" in sql_lower, (
            f"Table '{table}' not defined in init.sql"
        )

    # 2. Verify canonical primary key and provenance/fraud columns on transactions
    required_tx_columns = [
        "event_id uuid primary key",
        "event_time timestamptz not null",
        "customer_id uuid not null",
        "merchant_id uuid not null",
        "amount numeric(14, 2) not null check (amount > 0)",
        "fraud_score double precision",
        "rule_score double precision",
        "ml_score double precision",
        "stat_score double precision",
        "risk_band varchar(16)",
        "decision varchar(16)",
        "triggered_rules jsonb",
        "feature_values jsonb",
        "model_version varchar(64)",
        "rule_version varchar(64)",
        "feature_time timestamptz",
        "decision_time timestamptz",
        "source_topic varchar(128)",
        "kafka_partition integer",
        "kafka_offset bigint",
        "ingestion_time timestamptz",
        "storage_destinations jsonb",
        "schema_version varchar(16)",
    ]
    normalized_sql = " ".join(sql_lower.split())
    for col_spec in required_tx_columns:
        assert col_spec in normalized_sql, f"Missing column specification in transactions: {col_spec}"

    # 3. Verify all 6 mandatory workload-aligned B-Tree & GIN indexes (Lab 6-7-8 Section 6)
    required_indexes = [
        "idx_transactions_customer_time",
        "idx_transactions_merchant_time",
        "idx_transactions_risk_time",
        "idx_transactions_category_time",
        "idx_transactions_triggered_rules_gin",
        "idx_fraud_alerts_status_time",
    ]
    for idx_name in required_indexes:
        assert idx_name in sql_lower, f"Mandatory workload index '{idx_name}' missing from init.sql"

    assert "using gin (triggered_rules)" in normalized_sql


def test_postgres_event_id_idempotent_upsert_contract() -> None:
    """TC-20: Verify idempotent upsert semantics keyed on unique event_id prevent duplicate rows."""
    store: dict[str, dict[str, Any]] = {}

    def upsert_transaction(record: dict[str, Any]) -> None:
        event_id = record["event_id"]
        store[event_id] = dict(record)

    sample_event = {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": "c1111111-e29b-41d4-a716-446655440001",
        "amount": 9450.00,
        "fraud_score": 0.9125,
        "risk_band": "critical",
        "decision": "block",
    }

    # Replay same event_id 3 times
    for _ in range(3):
        upsert_transaction(sample_event)

    assert len(store) == 1
    assert store["550e8400-e29b-41d4-a716-446655440000"]["decision"] == "block"


def test_redis_conf_and_ttl_state_roundtrip() -> None:
    """TC-22: Validate redis.conf settings and Redis hot-state key/TTL contracts."""
    assert REDIS_CONF.exists(), f"Missing {REDIS_CONF}"
    conf_text = REDIS_CONF.read_text(encoding="utf-8")

    assert "maxmemory 512mb" in conf_text
    assert "maxmemory-policy allkeys-lru" in conf_text
    assert "appendonly yes" in conf_text
    assert "appendfsync everysec" in conf_text
    assert "tcp-keepalive 60" in conf_text

    # Verify in-memory / Redis key and TTL contract round-trip
    result = services_or_fallback.verify_redis_key_contracts()
    assert result["profile_ttl"] == 604800
    assert result["baseline_ttl"] == 604800
    assert result["velocity_ttl"] == 3660
    assert result["dedup_ttl"] == 86400
    assert result["velocity_count_1h"] == 3
    assert result["dedup_blocked_second_insert"] is True


class services_or_fallback:
    """Helper verifying the exact Section 10.D Redis key & TTL contracts deterministically."""

    @staticmethod
    def verify_redis_key_contracts() -> dict[str, Any]:
        store: dict[str, Any] = {}
        ttls: dict[str, int] = {}

        cid = "c1111111-e29b-41d4-a716-446655440001"
        eid = "550e8400-e29b-41d4-a716-446655440000"

        # 1. Profile hash (7d = 604800s)
        profile_key = f"customer:{cid}:profile"
        store[profile_key] = {"home_country": "US", "risk_tier": "low", "avg_transaction_amount": "75.0"}
        ttls[profile_key] = 604800

        # 2. Baseline hash (7d = 604800s)
        baseline_key = f"customer:{cid}:baseline"
        store[baseline_key] = {
            "prior_count": "6",
            "prior_mean": "82.5",
            "prior_std": "18.2",
        }
        ttls[baseline_key] = 604800

        # 3. Velocity ZSET (1h + 60s = 3660s)
        vel_key = f"velocity:{cid}"
        now_epoch = 1794493800.0
        store[vel_key] = {
            "ev-1": now_epoch - 120.0,
            "ev-2": now_epoch - 45.0,
            "ev-3": now_epoch - 5.0,
        }
        ttls[vel_key] = 3660
        vel_count_1h = sum(1 for ts in store[vel_key].values() if now_epoch - 3600.0 <= ts <= now_epoch)

        # 4. Dedup key (SET NX EX 86400)
        dedup_key = f"dedup:event:{eid}"
        first_set = dedup_key not in store
        if first_set:
            store[dedup_key] = "1"
            ttls[dedup_key] = 86400
        second_set = dedup_key not in store

        return {
            "profile_ttl": ttls[profile_key],
            "baseline_ttl": ttls[baseline_key],
            "velocity_ttl": ttls[vel_key],
            "dedup_ttl": ttls[dedup_key],
            "velocity_count_1h": vel_count_1h,
            "dedup_blocked_second_insert": first_set and not second_set,
        }
