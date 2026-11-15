"""
Unit tests for Idempotent Polyglot Persistence Sinks (`postgres_sink.py`, `timescale_sink.py`, `redis_sink.py`)
(`STEP AARUSSH-MOD02-03` / `TC-20`, `TC-21`, `TC-22`).
"""
from __future__ import annotations

from pathlib import Path
import sys
from typing import Any, Dict, List
import uuid

import pytest

SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

from app.sinks.postgres_sink import (  # noqa: E402
    UPSERT_FRAUD_ALERT_SQL,
    UPSERT_TRANSACTION_SQL,
    PostgresOperationalSink,
)
from app.sinks.redis_sink import RedisStateSink  # noqa: E402
from app.sinks.timescale_sink import (  # noqa: E402
    UPSERT_CATEGORY_METRICS_SQL,
    UPSERT_GEO_METRICS_SQL,
    UPSERT_TRANSACTION_METRICS_SQL,
    TimescaleMetricsSink,
)


def _make_decision_record(
    event_id: str | None = None,
    customer_id: str = "c1111111-e29b-41d4-a716-446655440001",
    merchant_id: str = "a2222222-e29b-41d4-a716-446655440002",
    event_time: str = "2026-11-12T14:30:12Z",
    amount: float = 250.0,
    decision: str = "approve",
    risk_band: str = "low",
    fraud_score: float = 0.12,
    category: str = "electronics",
    country: str = "US",
    city: str = "New York",
) -> Dict[str, Any]:
    ev_id = event_id or str(uuid.uuid4())
    return {
        "event_id": ev_id,
        "transaction_id": ev_id,
        "event_time": event_time,
        "customer_id": customer_id,
        "merchant_id": merchant_id,
        "amount": amount,
        "currency": "USD",
        "channel": "online",
        "category": category,
        "location": {
            "country": country,
            "city": city,
            "latitude": 40.7128,
            "longitude": -74.0060,
        },
        "fraud_score": fraud_score,
        "rule_score": 0.85 if decision in ("flag", "block") else 0.0,
        "ml_score": 0.90 if decision in ("flag", "block") else 0.10,
        "stat_score": 0.70 if decision in ("flag", "block") else 0.05,
        "z_score_signal": 0.70 if decision in ("flag", "block") else 0.05,
        "risk_band": risk_band,
        "risk_level": risk_band,
        "decision": decision,
        "triggered_rules": ["GEO_IMPOSSIBILITY"] if decision == "block" else [],
        "reasons": ["GEO_IMPOSSIBILITY: speed > 900 km/h"] if decision == "block" else [],
        "model_version": "xgboost-v1.0.0",
        "shadow_model_version": "heuristic-shadow-v1.0.0",
        "shadow_ml_score": 0.82 if decision == "block" else 0.09,
        "rule_version": "rules-v1.0.0",
        "transformation_version": "features-v1.0.0",
        "schema_version": "1.0",
        "feature_time": "2026-11-12T14:30:12.010Z",
        "decision_time": "2026-11-12T14:30:12.025Z",
        "processing_time_ms": 15,
        "feature_values": {
            "txn_count_1m": 2,
            "amount_z_score": 4.5 if decision == "block" else 0.2,
            "is_warmup": False,
        },
        "provenance": {
            "source_topic": "hermes.transactions.raw",
            "partition": 3,
            "offset": 4096,
            "ingestion_time": "2026-11-12T14:30:12.002Z",
            "storage_destinations": [
                "postgres:transactions",
                "postgres:fraud_alerts",
                "redis:customer_state",
                "minio:parquet",
            ],
        },
    }


def test_postgres_upsert_idempotency_and_provenance() -> None:
    """
    TC-20 & TC-22 (`NFR-05`, `NFR-07`): Replaying the same batch of 5 FraudDecision records
    twice produces zero duplicate `event_id` rows in `PostgresOperationalSink` and preserves
    100% of audit provenance columns.
    """
    assert "ON CONFLICT (event_id) DO UPDATE" in UPSERT_TRANSACTION_SQL
    assert "ON CONFLICT (event_id) DO UPDATE" in UPSERT_FRAUD_ALERT_SQL

    pg_sink = PostgresOperationalSink()
    batch: List[Dict[str, Any]] = [
        _make_decision_record(amount=45.0, decision="approve", risk_band="low", fraud_score=0.05),
        _make_decision_record(amount=80.0, decision="approve", risk_band="low", fraud_score=0.11),
        _make_decision_record(amount=420.0, decision="approve", risk_band="medium", fraud_score=0.45),
        _make_decision_record(amount=1800.0, decision="flag", risk_band="high", fraud_score=0.74),
        _make_decision_record(amount=9450.0, decision="block", risk_band="critical", fraud_score=0.93),
    ]

    # First batch write
    tx_rows_1, alert_rows_1 = pg_sink.upsert_batch(batch)
    assert len(tx_rows_1) == 5
    assert len(alert_rows_1) == 2
    assert len(pg_sink.transactions_by_event_id) == 5
    assert len(pg_sink.alerts_by_event_id) == 2

    # Replay the exact same 5 events (simulating Kafka offset rewind after checkpoint recovery)
    tx_rows_2, alert_rows_2 = pg_sink.upsert_batch(batch)
    assert len(tx_rows_2) == 5
    assert len(alert_rows_2) == 2
    # Unique stored rows MUST remain 5 transactions and 2 alerts (zero duplicates!)
    assert len(pg_sink.transactions_by_event_id) == 5
    assert len(pg_sink.alerts_by_event_id) == 2

    blocked_id = batch[4]["event_id"]
    stored_tx = pg_sink.get_transaction(blocked_id)
    assert stored_tx is not None
    assert stored_tx["source_topic"] == "hermes.transactions.raw"
    assert stored_tx["kafka_partition"] == 3
    assert stored_tx["kafka_offset"] == 4096
    assert stored_tx["model_version"] == "xgboost-v1.0.0"
    assert stored_tx["rule_version"] == "rules-v1.0.0"
    assert stored_tx["transformation_version"] == "features-v1.0.0"
    assert stored_tx["schema_version"] == "1.0"
    assert stored_tx["triggered_rules"] == ["GEO_IMPOSSIBILITY"]
    assert "postgres:fraud_alerts" in stored_tx["storage_destinations"]

    stored_alert = pg_sink.get_alert_by_event_id(blocked_id)
    assert stored_alert is not None
    assert stored_alert["alert_id"] == f"alert-{blocked_id}"
    assert stored_alert["status"] == "active"


def test_timescaledb_metrics_sink_hypertable_rollups() -> None:
    """
    TC-21 (`MOD-05-FN-02`): Verifies `TimescaleMetricsSink` aggregates scored transactions
    into `transaction_metrics`, `category_metrics`, and `geo_metrics` 1m buckets and
    suppresses duplicate `event_id`s on replay.
    """
    assert "ON CONFLICT (bucket_time, window_size, channel, region) DO UPDATE" in UPSERT_TRANSACTION_METRICS_SQL
    assert "ON CONFLICT (bucket_time, category) DO UPDATE" in UPSERT_CATEGORY_METRICS_SQL
    assert "ON CONFLICT (bucket_time, country, city) DO UPDATE" in UPSERT_GEO_METRICS_SQL

    ts_sink = TimescaleMetricsSink()
    d1 = _make_decision_record(
        event_time="2026-11-12T14:30:05Z",
        amount=100.0,
        decision="approve",
        risk_band="low",
        category="groceries",
        country="US",
        city="New York",
    )
    d2 = _make_decision_record(
        event_time="2026-11-12T14:30:40Z",
        amount=900.0,
        decision="block",
        risk_band="critical",
        category="electronics",
        country="RU",
        city="Moscow",
    )

    # Include d1 twice to verify duplicate event_id suppression
    res = ts_sink.ingest_scored_decisions([d1, d2, d1], window_size="1m")

    assert len(res["transaction_metrics"]) == 1
    tx_m = res["transaction_metrics"][0]
    assert tx_m["bucket_time"] == "2026-11-12T14:30:00Z"
    assert tx_m["tx_count"] == 2
    assert tx_m["total_amount"] == pytest.approx(1000.0)
    assert tx_m["avg_amount"] == pytest.approx(500.0)
    assert tx_m["fraud_count"] == 1
    assert tx_m["blocked_count"] == 1
    assert tx_m["fraud_rate"] == pytest.approx(0.5)

    assert len(res["category_metrics"]) == 2
    assert len(res["geo_metrics"]) == 2


def test_redis_state_sink_updates_velocity_and_dedup() -> None:
    """
    TC-22 (`MOD-05-FN-03`): Verifies `RedisStateSink` updates `customer:{id}:latest_decision`,
    `velocity:{id}` ZSET, and `dedup:event:{event_id}`.
    """
    redis_sink = RedisStateSink()
    cust_id = "c1111111-e29b-41d4-a716-446655440001"
    d1 = _make_decision_record(
        customer_id=cust_id,
        event_time="2026-11-12T14:30:00Z",
        amount=120.0,
        decision="approve",
    )
    d2 = _make_decision_record(
        customer_id=cust_id,
        event_time="2026-11-12T14:30:30Z",
        amount=3400.0,
        decision="flag",
        risk_band="high",
        fraud_score=0.78,
    )

    redis_sink.persist_batch([d1, d2])

    latest = redis_sink.customer_latest_decision[cust_id]
    assert latest["event_id"] == d2["event_id"]
    assert latest["decision"] == "flag"
    assert f"dedup:event:{d1['event_id']}" in redis_sink.dedup_keys
    assert f"dedup:event:{d2['event_id']}" in redis_sink.dedup_keys
    assert len(redis_sink.velocity_zsets[cust_id]) == 2
