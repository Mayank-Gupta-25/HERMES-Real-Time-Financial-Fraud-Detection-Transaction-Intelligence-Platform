from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402
from app.middleware.auth import create_access_token  # noqa: E402
from app.services.kafka_producer import GatewayKafkaProducer  # noqa: E402


def test_post_valid_transaction_publishes_to_kafka_with_customer_key() -> None:
    """TC-04, TC-06: Verifies POST /api/v1/transactions with a valid JWT returns 202 Accepted
    and publishes to `hermes.transactions.raw` partitioned by `customer_id`.
    """
    mock_confluent = MagicMock()
    mock_producer = GatewayKafkaProducer(producer_instance=mock_confluent)
    app.state.kafka_producer = mock_producer

    client = TestClient(app)
    token = create_access_token(subject="ingest-worker-01")

    event_id = str(uuid.uuid4())
    customer_id = str(uuid.uuid4())
    merchant_id = str(uuid.uuid4())

    payload = {
        "event_id": event_id,
        "event_time": "2026-11-15T14:30:00Z",
        "customer_id": customer_id,
        "merchant_id": merchant_id,
        "amount": 340.75,
        "currency": "USD",
        "channel": "online",
        "card_type": "credit",
        "category": "electronics",
        "location": {
            "country": "US",
            "city": "San Francisco",
            "latitude": 37.7749,
            "longitude": -122.4194,
        },
        "device": {
            "device_id": "dev-sf-101",
            "device_type": "desktop",
            "os": "Windows",
            "ip_address": "198.51.100.42",
        },
    }

    response = client.post(
        "/api/v1/transactions",
        json=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "X-Correlation-ID": "corr-tc06-001",
        },
    )

    assert response.status_code == 202
    data = response.json()
    assert data["event_id"] == event_id
    assert data["transaction_id"] == event_id
    assert data["status"] == "accepted"
    assert data["schema_version"] == "1.0"
    assert data["topic"] == "hermes.transactions.raw"
    assert data["correlation_id"] == "corr-tc06-001"
    assert isinstance(data["partition"], int)
    assert isinstance(data["kafka_offset"], int)

    # Assert underlying confluent producer was called with topic hermes.transactions.raw and key=customer_id
    assert mock_confluent.produce.call_count == 1
    call_kwargs = mock_confluent.produce.call_args.kwargs
    assert call_kwargs["topic"] == "hermes.transactions.raw"
    assert call_kwargs["key"] == customer_id.encode("utf-8")
    assert len(mock_producer.published_transactions) == 1
    assert mock_producer.published_transactions[0]["key"] == customer_id


def test_post_invalid_transaction_routes_to_dlq_with_reason_code() -> None:
    """TC-08: Posts a payload with `amount: -15.0` and verifies HTTP 422 response with
    `error_code == 'ERR_INVALID_AMOUNT'`, `dlq_routed == True`, and asserts `publish_dlq` was called once.
    """
    mock_confluent = MagicMock()
    mock_producer = GatewayKafkaProducer(producer_instance=mock_confluent)
    mock_producer.publish_dlq = MagicMock(wraps=mock_producer.publish_dlq)  # type: ignore[method-assign]
    app.state.kafka_producer = mock_producer

    client = TestClient(app)
    token = create_access_token(subject="ingest-worker-01")

    invalid_payload = {
        "event_id": str(uuid.uuid4()),
        "event_time": "2026-11-15T14:30:00Z",
        "customer_id": str(uuid.uuid4()),
        "merchant_id": str(uuid.uuid4()),
        "amount": -15.0,
        "currency": "USD",
        "category": "groceries",
        "location": {
            "country": "US",
            "city": "Boston",
            "latitude": 42.3601,
            "longitude": -71.0589,
        },
    }

    response = client.post(
        "/api/v1/transactions",
        json=invalid_payload,
        headers={
            "Authorization": f"Bearer {token}",
            "X-Correlation-ID": "corr-dlq-008",
        },
    )

    assert response.status_code == 422
    body = response.json()
    assert body["error_code"] == "ERR_INVALID_AMOUNT"
    assert body["dlq_routed"] is True
    assert body["correlation_id"] == "corr-dlq-008"

    assert mock_producer.publish_dlq.call_count == 1
    assert len(mock_producer.published_dlq_events) == 1
    dlq_record = mock_producer.published_dlq_events[0]
    assert dlq_record["error_code"] == "ERR_INVALID_AMOUNT"
    assert dlq_record["source_service"] == "api-gateway"
    assert dlq_record["source_topic"] == "hermes.transactions.raw"


def test_health_and_metrics_endpoints() -> None:
    """Verifies `/health`, `/readiness`, `/metrics`, and `/api/v1/auth/token` return HTTP 200."""
    app.state.kafka_producer = GatewayKafkaProducer(producer_instance=MagicMock())
    client = TestClient(app)

    health_resp = client.get("/health")
    assert health_resp.status_code == 200
    assert health_resp.json() == {"status": "ok", "service": settings.service_name}

    ready_resp = client.get("/readiness")
    assert ready_resp.status_code == 200
    assert ready_resp.json()["status"] == "ready"
    assert ready_resp.json()["checks"]["kafka"] is True

    metrics_resp = client.get("/metrics")
    assert metrics_resp.status_code == 200
    assert "hermes_transactions_ingested_total" in metrics_resp.text
    assert "hermes_api_request_duration_ms" in metrics_resp.text

    token_resp = client.post("/api/v1/auth/token", json={"client_id": "locust-runner"})
    assert token_resp.status_code == 200
    assert "access_token" in token_resp.json()
    assert token_resp.json()["token_type"] == "bearer"
