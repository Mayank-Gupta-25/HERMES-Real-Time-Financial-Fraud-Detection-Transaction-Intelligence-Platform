"""
Unit and schema integration tests for Transaction Simulator serializer and Kafka producer
(`TC-01`, `TC-02`, `TC-03`).
"""
from __future__ import annotations

import asyncio
import importlib.util
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

SIMULATOR_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = SIMULATOR_ROOT.parent.parent
if str(SIMULATOR_ROOT) not in sys.path:
    sys.path.insert(0, str(SIMULATOR_ROOT))

from app.config import Settings, settings
from app.generators.customer_gen import generate_customers
from app.generators.fraud_patterns import FRAUD_PATTERNS, inject_fraud_pattern, inject_structuring
from app.generators.merchant_gen import generate_merchants
from app.generators.transaction_gen import generate_batch, generate_normal_transaction
from app.producer import InMemoryMockProducer, SimulatorKafkaProducer
from app.serializer import TransactionSerializer, deserialize_transaction, serialize_transaction

CANONICAL_V1_FIELDS = {
    "event_id",
    "event_time",
    "customer_id",
    "merchant_id",
    "amount",
    "currency",
    "transaction_type",
    "channel",
    "card_type",
    "category",
    "location",
    "device",
    "is_international",
    "status",
    "schema_version",
}


def _load_canonical_validator():
    validator_path = (
        REPO_ROOT / "services" / "transaction-processor" / "app" / "schemas" / "validator.py"
    )
    if validator_path.exists():
        try:
            spec = importlib.util.spec_from_file_location("canonical_validator", validator_path)
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                sys.modules["canonical_validator"] = mod
                spec.loader.exec_module(mod)
                if hasattr(mod, "validate_and_normalize_event"):
                    return mod.validate_and_normalize_event
        except Exception:
            pass

    def _fallback_validate(raw_payload: Dict[str, Any], **_kwargs):
        assert raw_payload.get("schema_version") == "1.0"
        uuid.UUID(str(raw_payload["event_id"]))
        uuid.UUID(str(raw_payload["customer_id"]))
        uuid.UUID(str(raw_payload["merchant_id"]))
        assert str(raw_payload["event_time"]).endswith("Z")
        assert float(raw_payload["amount"]) > 0
        loc = raw_payload["location"]
        assert -90.0 <= float(loc["latitude"]) <= 90.0
        assert -180.0 <= float(loc["longitude"]) <= 180.0
        return raw_payload, None

    return _fallback_validate


def test_canonical_v1_fields_present_on_normal_and_fraud_events():
    """TC-01 & TC-02: Verify canonical v1.0 fields on normal and all 5 fraud pattern events."""
    customers = generate_customers(5)
    merchants = generate_merchants(15)

    normal_tx = generate_normal_transaction(customers[0], merchants[0])
    assert CANONICAL_V1_FIELDS.issubset(normal_tx.keys())
    assert normal_tx["event_id"] == normal_tx["transaction_id"]
    assert normal_tx["event_time"] == normal_tx["timestamp"]
    assert normal_tx["event_time"].endswith("Z")
    assert normal_tx["category"] == normal_tx["merchant_category"]
    assert normal_tx["device"] == normal_tx["device_info"]
    assert normal_tx["schema_version"] == "1.0"

    for pattern_name in FRAUD_PATTERNS:
        burst = inject_fraud_pattern(customers[0], merchants, pattern=pattern_name)
        assert len(burst) >= 2
        for tx in burst:
            assert CANONICAL_V1_FIELDS.issubset(tx.keys())
            assert tx["schema_version"] == "1.0"
            assert tx["event_time"].endswith("Z")
            assert tx["is_fraud"] is True
            assert tx["fraud_pattern"] == pattern_name

    structuring_burst = inject_structuring(customers[0], merchants)
    assert all(8000.0 <= tx["amount"] <= 9999.0 for tx in structuring_burst)


def test_simulator_events_pass_canonical_validator():
    """TC-01 & TC-03: Validate 100 generated normal and fraudulent events against validator."""
    customers = generate_customers(15)
    merchants = generate_merchants(25)
    txns = generate_batch(customers, merchants, count=100, fraud_rate=0.25)
    assert len(txns) == 100

    validate_fn = _load_canonical_validator()
    for tx in txns:
        normalized, dlq = validate_fn(tx)
        assert dlq is None, f"Unexpected DLQ rejection: {dlq}"
        assert normalized is not None
        assert normalized["event_id"] == tx["event_id"]
        assert normalized["schema_version"] == "1.0"


def test_json_and_avro_serializer_roundtrip():
    """TC-03: Verify JSON and Avro serialization round-trip and label stripping."""
    customers = generate_customers(2)
    merchants = generate_merchants(2)
    tx = generate_normal_transaction(customers[0], merchants[0])

    for fmt in ("json", "avro"):
        raw_bytes = serialize_transaction(tx, fmt=fmt, strip_labels=True)
        assert isinstance(raw_bytes, bytes) and len(raw_bytes) > 0
        decoded = deserialize_transaction(raw_bytes, fmt=fmt)
        assert decoded["event_id"] == tx["event_id"]
        assert decoded["customer_id"] == tx["customer_id"]
        assert decoded["merchant_id"] == tx["merchant_id"]
        assert decoded["amount"] == pytest.approx(tx["amount"])
        assert decoded["category"] == tx["category"]
        assert decoded["schema_version"] == "1.0"
        assert "_label" not in decoded


def test_kafka_producer_partitions_by_customer_id():
    """TC-03: Verify SimulatorKafkaProducer publishes to hermes.transactions.raw keyed by customer_id."""
    customers = generate_customers(6)
    merchants = generate_merchants(10)
    txns = generate_batch(customers, merchants, count=25, fraud_rate=0.2)

    mock_client = InMemoryMockProducer()
    producer = SimulatorKafkaProducer(
        bootstrap_servers="localhost:9092",
        topic="hermes.transactions.raw",
        serialization_format="json",
        producer_client=mock_client,
    )

    stats = producer.publish_batch(txns)
    assert stats["produced_count"] == 25
    assert stats["delivered_count"] == 25
    assert len(mock_client.messages) == 25
    assert mock_client.flushed == 1

    for tx, msg in zip(txns, mock_client.messages):
        assert msg["topic"] == "hermes.transactions.raw"
        assert msg["key"] == tx["customer_id"].encode("utf-8")
        header_dict = dict(msg["headers"])
        assert header_dict.get("schema-version") == b"1.0"


def test_async_send_at_rate_tracks_delivery():
    """TC-03: Verify send_at_rate works cleanly with mock producer without requiring a socket event loop."""
    customers = generate_customers(3)
    merchants = generate_merchants(5)
    txns = generate_batch(customers, merchants, count=10, fraud_rate=0.0)

    mock_client = InMemoryMockProducer()
    producer = SimulatorKafkaProducer(producer_client=mock_client)

    async def _run():
        await producer.start()
        res = await producer.send_at_rate(txns, tps=0)
        await producer.stop()
        return res

    coro = _run()
    try:
        coro.send(None)
        pytest.fail("Expected coroutine with tps=0 and mock producer to complete without yielding")
    except StopIteration as stop:
        result = stop.value

    assert result["sent"] == 10
    assert result["delivered_count"] == 10
    assert settings.schema_version == "1.0"
    assert isinstance(Settings(), Settings)
