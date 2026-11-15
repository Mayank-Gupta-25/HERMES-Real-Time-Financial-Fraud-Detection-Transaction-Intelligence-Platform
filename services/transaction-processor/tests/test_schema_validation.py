"""
Unit tests for Canonical Avro Schemas, Deterministic Event Validator & DLQ Routing
(`MOD-01-FN-07` / `TC-07` and `MOD-01-FN-08` / `TC-08`).
"""
from __future__ import annotations

import copy
import sys
import uuid
from pathlib import Path
from typing import Any, Dict

import pytest

SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

from app.schemas.avro_schemas import (  # noqa: E402
    FRAUD_DECISION_SCHEMA_V1,
    TRANSACTION_EVENT_SCHEMA_V1,
    deserialize_avro,
    serialize_avro,
)
from app.schemas.validator import (  # noqa: E402
    CANONICAL_ERROR_CODES,
    validate_and_normalize_event,
)


def _make_valid_event(**overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "event_time": "2026-11-12T20:00:00+05:30",
        "customer_id": "c1111111-e29b-41d4-a716-446655440001",
        "merchant_id": "m2222222-e29b-41d4-a716-446655440002".replace("m", "a"),
        "amount": 149.95,
        "currency": "usd",
        "transaction_type": "purchase",
        "channel": "online",
        "card_type": "credit",
        "category": "Electronics",
        "location": {
            "country": "in",
            "city": "Mumbai",
            "latitude": 19.0760,
            "longitude": 72.8777,
        },
        "device": {
            "device_id": None,
            "ip_address": "203.0.113.42",
            "device_type": "mobile",
            "os": None,
        },
        "is_international": False,
        "status": "pending",
        "schema_version": "1.0",
    }
    base.update(overrides)
    return base


def test_valid_event_normalization_and_avro_roundtrip() -> None:
    """
    TC-07: Verifies a valid event normalizes timezone-offset timestamps to UTC 'Z',
    normalizes currency/country/category casing, preserves None in optional device
    fields without dummy imputation, and round-trips cleanly through Avro serialization.
    """
    assert TRANSACTION_EVENT_SCHEMA_V1["name"] == "TransactionEvent"
    assert FRAUD_DECISION_SCHEMA_V1["name"] == "FraudDecision"

    raw = _make_valid_event()
    normalized, dlq = validate_and_normalize_event(raw)

    assert dlq is None
    assert normalized is not None
    # 20:00:00+05:30 -> 14:30:00Z
    assert normalized["event_time"] == "2026-11-12T14:30:00Z"
    assert normalized["currency"] == "USD"
    assert normalized["category"] == "electronics"
    assert normalized["location"]["country"] == "IN"
    # Optional device fields must stay explicitly None (no dummy imputation)
    assert normalized["device"]["device_id"] is None
    assert normalized["device"]["os"] is None
    assert normalized["device"]["ip_address"] == "203.0.113.42"

    # Avro round-trip
    wire_bytes = serialize_avro(normalized, schema_name="transaction_v1")
    assert isinstance(wire_bytes, bytes)
    decoded = deserialize_avro(wire_bytes, schema_name="transaction_v1")
    assert decoded["event_id"] == normalized["event_id"]
    assert decoded["event_time"] == "2026-11-12T14:30:00Z"
    assert decoded["amount"] == pytest.approx(149.95)
    assert decoded["device"]["device_id"] is None
    assert decoded["device"]["os"] is None

    # Also verify top-level device=None is preserved
    raw_no_device = _make_valid_event(device=None)
    norm_no_dev, dlq_no_dev = validate_and_normalize_event(raw_no_device)
    assert dlq_no_dev is None
    assert norm_no_dev is not None
    assert norm_no_dev["device"] is None


def test_legacy_field_aliases_normalized() -> None:
    """
    TC-07 / DIS-02: Verifies legacy simulator/API aliases (`transaction_id`, `timestamp`,
    `merchant_category`, `device_info`) map cleanly to `event_id`, `event_time`, `category`, `device`.
    """
    legacy_payload = {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": "2026-11-12T14:30:00Z",
        "customer_id": str(uuid.uuid4()),
        "merchant_id": str(uuid.uuid4()),
        "amount": 82.50,
        "currency": "USD",
        "transaction_type": "purchase",
        "channel": "pos",
        "card_type": "debit",
        "merchant_category": "Groceries",
        "location": {
            "country": "US",
            "city": "New York",
            "latitude": 40.7128,
            "longitude": -74.0060,
        },
        "device_info": {
            "ip_address": "198.51.100.12",
            "device_type": "mobile",
        },
    }

    normalized, dlq = validate_and_normalize_event(legacy_payload)
    assert dlq is None
    assert normalized is not None
    assert normalized["event_id"] == legacy_payload["transaction_id"]
    assert normalized["event_time"] == "2026-11-12T14:30:00Z"
    assert normalized["category"] == "groceries"
    assert normalized["device"] is not None
    assert normalized["device"]["ip_address"] == "198.51.100.12"
    assert normalized["device"]["device_id"] is None
    assert normalized["device"]["os"] is None
    assert normalized["schema_version"] == "1.0"


def test_high_value_outlier_preserved() -> None:
    """
    TC-07 / FR-02 / DIS-05: Verifies a $5,000,000.00 high-value transaction is NEVER dropped
    at the ingestion/validation boundary.
    """
    high_value_event = _make_valid_event(amount=5_000_000.00)
    normalized, dlq = validate_and_normalize_event(high_value_event)

    assert dlq is None
    assert normalized is not None
    assert normalized["amount"] == pytest.approx(5_000_000.00)


@pytest.mark.parametrize(
    "mutator,expected_error_code",
    [
        (lambda e: e.update({"amount": -15.0}), "ERR_INVALID_AMOUNT"),
        (lambda e: e.update({"amount": 0.0}), "ERR_INVALID_AMOUNT"),
        (lambda e: e.update({"amount": float("nan")}), "ERR_INVALID_AMOUNT"),
        (lambda e: e.update({"amount": float("inf")}), "ERR_INVALID_AMOUNT"),
        (lambda e: e.update({"event_id": "not-a-valid-uuid"}), "ERR_INVALID_UUID"),
        (lambda e: e.update({"customer_id": "bad-customer-uuid"}), "ERR_INVALID_UUID"),
        (lambda e: e.update({"event_time": "not-a-timestamp"}), "ERR_INVALID_TIMESTAMP"),
        (
            lambda e: e["location"].update({"latitude": 95.5}),
            "ERR_INVALID_COORDINATES",
        ),
        (
            lambda e: e["location"].update({"longitude": -195.0}),
            "ERR_INVALID_COORDINATES",
        ),
        (lambda e: e.update({"schema_version": "2.0"}), "ERR_UNSUPPORTED_SCHEMA_VERSION"),
        (lambda e: e.pop("merchant_id", None), "ERR_MISSING_REQUIRED_FIELD"),
        (lambda e: e.update({"channel": "carrier_pigeon"}), "ERR_INVALID_CATEGORY_OR_ENUM"),
    ],
)
def test_invalid_events_produce_dlq_with_reason_codes(
    mutator: Any, expected_error_code: str
) -> None:
    """
    TC-07 & TC-08: Every invalid event produces `(None, dlq_event)` with canonical `error_code`,
    original payload, ingestion timestamp, schema_version, and source topic.
    """
    invalid_payload = copy.deepcopy(_make_valid_event())
    mutator(invalid_payload)

    normalized, dlq = validate_and_normalize_event(
        invalid_payload,
        source_topic="hermes.transactions.raw",
        source_service="transaction-processor",
    )

    assert normalized is None
    assert dlq is not None
    assert dlq["error_code"] == expected_error_code
    assert dlq["error_code"] in CANONICAL_ERROR_CODES
    assert dlq["dlq_id"].startswith("dlq-")
    assert dlq["source_topic"] == "hermes.transactions.raw"
    assert dlq["source_service"] == "transaction-processor"
    assert dlq["ingestion_time"].endswith("Z")
    assert isinstance(dlq["original_payload"], str) and len(dlq["original_payload"]) > 0


def test_malformed_raw_bytes_routed_to_dlq() -> None:
    """TC-08: Corrupt binary/JSON string produces ERR_MALFORMED_PAYLOAD DLQ record."""
    normalized, dlq = validate_and_normalize_event("{corrupt-json")
    assert normalized is None
    assert dlq is not None
    assert dlq["error_code"] == "ERR_MALFORMED_PAYLOAD"
    assert "{corrupt-json" in dlq["original_payload"]
