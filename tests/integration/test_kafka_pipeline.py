"""
Integration test for End-to-End Kafka Streaming Pipeline Conservation and Persistence.
Test ID: TC-28 (Pipeline Conservation & DLQ Partitioning)
Owner: Aarussh
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
import sys
from typing import Any, Dict, List
import uuid

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSOR_ROOT = REPO_ROOT / "services" / "transaction-processor"
SIMULATOR_ROOT = REPO_ROOT / "services" / "transaction-simulator"

for p in (str(PROCESSOR_ROOT), str(SIMULATOR_ROOT), str(REPO_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from services.transaction_processor.app.config import settings
from services.transaction_processor.app.main import TransactionStreamPipeline
from services.transaction_simulator.app.generators.customer_gen import generate_customers
from services.transaction_simulator.app.generators.merchant_gen import generate_merchants
from services.transaction_simulator.app.generators.transaction_gen import generate_batch


def test_kafka_pipeline_50_transactions_and_5_dlq_conservation(tmp_path: Path) -> None:
    """
    TC-28: End-to-end stream pipeline processing of 50 transactions (including fraud patterns)
    plus 5 malformed payloads, asserting exact conservation:
    produced (55) == valid_unique (50) + dlq (5).
    """
    random.seed(42)
    customers = generate_customers(count=15)
    merchants = generate_merchants(count=10)

    # 1. Generate 50 transactions with fraud pattern bursts
    valid_txns = generate_batch(customers, merchants, count=50, fraud_rate=0.20, profile_name="normal")
    assert len(valid_txns) == 50

    # Ensure all 50 have unique event_ids and valid timestamps
    seen_ids = set()
    base_time = datetime(2026, 11, 12, 14, 0, 0, tzinfo=timezone.utc)
    for idx, tx in enumerate(valid_txns):
        if "event_id" not in tx or tx["event_id"] in seen_ids:
            tx["event_id"] = str(uuid.uuid4())
            tx["transaction_id"] = tx["event_id"]
        seen_ids.add(tx["event_id"])
        # Incremental timestamps
        ts = (base_time + timedelta(seconds=idx * 4)).isoformat().replace("+00:00", "Z")
        tx["event_time"] = ts
        tx["timestamp"] = ts
        tx["schema_version"] = "1.0"

    # 2. Construct 5 malformed payloads
    bad_payloads: List[Dict[str, Any]] = [
        # 1. Negative amount
        {
            "event_id": str(uuid.uuid4()),
            "event_time": "2026-11-12T14:05:00Z",
            "customer_id": customers[0].customer_id,
            "merchant_id": merchants[0].merchant_id,
            "amount": -50.0,
            "currency": "USD",
            "category": "retail",
            "location": {"country": "US", "city": "New York", "latitude": 40.7128, "longitude": -74.0060},
            "schema_version": "1.0",
        },
        # 2. Invalid UUID
        {
            "event_id": "not-a-valid-uuid-1234",
            "event_time": "2026-11-12T14:05:01Z",
            "customer_id": customers[0].customer_id,
            "merchant_id": merchants[0].merchant_id,
            "amount": 75.0,
            "currency": "USD",
            "category": "retail",
            "location": {"country": "US", "city": "New York", "latitude": 40.7128, "longitude": -74.0060},
            "schema_version": "1.0",
        },
        # 3. Bad timestamp
        {
            "event_id": str(uuid.uuid4()),
            "event_time": "corrupt-timestamp",
            "customer_id": customers[0].customer_id,
            "merchant_id": merchants[0].merchant_id,
            "amount": 120.0,
            "currency": "USD",
            "category": "groceries",
            "location": {"country": "US", "city": "New York", "latitude": 40.7128, "longitude": -74.0060},
            "schema_version": "1.0",
        },
        # 4. Invalid latitude
        {
            "event_id": str(uuid.uuid4()),
            "event_time": "2026-11-12T14:05:02Z",
            "customer_id": customers[0].customer_id,
            "merchant_id": merchants[0].merchant_id,
            "amount": 90.0,
            "currency": "USD",
            "category": "dining",
            "location": {"country": "US", "city": "North Pole", "latitude": 105.0, "longitude": 0.0},
            "schema_version": "1.0",
        },
        # 5. Unsupported schema version
        {
            "event_id": str(uuid.uuid4()),
            "event_time": "2026-11-12T14:05:03Z",
            "customer_id": customers[0].customer_id,
            "merchant_id": merchants[0].merchant_id,
            "amount": 45.0,
            "currency": "USD",
            "category": "entertainment",
            "location": {"country": "US", "city": "Boston", "latitude": 42.3601, "longitude": -71.0589},
            "schema_version": "9.9",
        },
    ]

    total_stream = valid_txns + bad_payloads
    assert len(total_stream) == 55

    # Seed customer and merchant profiles in Redis lookup
    pipeline = TransactionStreamPipeline(checkpoint_dir=tmp_path / "checkpoints")
    for c in customers:
        pipeline.redis_lookup.seed_customer_profile(
            c.customer_id,
            {
                "home_country": c.home_country,
                "home_city": c.home_city,
                "home_latitude": c.home_latitude,
                "home_longitude": c.home_longitude,
                "risk_tier": c.risk_tier,
                "avg_transaction_amount": c.avg_transaction_amount,
                "card_type": c.card_type,
            },
        )
    for m in merchants:
        pipeline.redis_lookup.seed_merchant_profile(
            m.merchant_id,
            {
                "name": m.name,
                "category": m.category,
                "country": m.country,
                "city": m.city,
                "is_high_risk": m.is_high_risk,
                "is_blacklisted": getattr(m, "is_blacklisted", False),
            },
        )

    # 3. Process the entire stream through the pipeline
    decisions, dlq_events = pipeline.process_raw_batch(total_stream, start_offset=0, partition=0)

    # 4. Verify conservation invariant (NFR-04)
    assert len(decisions) == 50, f"Expected 50 valid unique decisions, got {len(decisions)}"
    assert len(dlq_events) == 5, f"Expected 5 DLQ rejected events, got {len(dlq_events)}"

    metrics = pipeline.get_metrics()
    assert metrics["produced_count"] == 55
    assert metrics["processed_unique_count"] == 50
    assert metrics["dlq_count"] == 5
    assert metrics["cleaning_dlq_rejected"] == 5
    assert (
        metrics["produced_count"]
        == metrics["processed_unique_count"]
        + metrics["duplicate_dropped_count"]
        + metrics["dlq_count"]
    )

    # 5. Verify DLQ reason codes
    dlq_error_codes = {ev["error_code"] for ev in dlq_events}
    assert "ERR_INVALID_AMOUNT" in dlq_error_codes
    assert "ERR_INVALID_UUID" in dlq_error_codes
    assert "ERR_INVALID_TIMESTAMP" in dlq_error_codes
    assert "ERR_INVALID_COORDINATES" in dlq_error_codes
    assert "ERR_UNSUPPORTED_SCHEMA_VERSION" in dlq_error_codes

    # 6. Verify Polyglot Sinks Persistence
    # PostgreSQL operational store must have exactly 50 transactions
    assert len(pipeline.postgres_sink.transactions_by_event_id) == 50

    # TimescaleDB metrics sink must have recorded 1m time-series rollups
    assert len(pipeline.timescale_sink.transaction_metrics) > 0
    total_metric_txns = sum(m["tx_count"] for m in pipeline.timescale_sink.transaction_metrics.values())
    assert total_metric_txns == 50

    # All decisions must have valid fraud_scores and provenance
    for dec in decisions:
        assert 0.0 <= dec["fraud_score"] <= 1.0
        assert dec["risk_band"] in ("low", "medium", "high", "critical")
        assert dec["decision"] in ("approve", "flag", "block")
        assert dec["provenance"]["source_topic"] == settings.topic_transactions_raw
        assert dec["provenance"]["partition"] == 0
        assert "feature_values" in dec
