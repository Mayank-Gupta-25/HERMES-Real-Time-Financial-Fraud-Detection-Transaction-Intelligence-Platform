from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from pydantic import ValidationError

SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

from app.config import settings  # noqa: E402
from app.middleware.auth import JWTAuthMiddleware, create_access_token  # noqa: E402
from app.middleware.logging import CorrelationIdLoggingMiddleware  # noqa: E402
from app.middleware.rate_limiter import RedisRateLimiterMiddleware  # noqa: E402
from app.models.response import DLQErrorResponse, TransactionAcceptedResponse  # noqa: E402
from app.models.transaction import DeviceModel, LocationModel, TransactionCreateRequest  # noqa: E402


def _build_middleware_test_app(
    *,
    rate_limit_per_minute: int = 1000,
    redis_client: Any = None,
    secret_key: str = "test-jwt-secret-key-hermes-2026",
) -> FastAPI:
    app = FastAPI(title="hermes-api-gateway-test")

    # Starlette executes middleware in reverse registration order (outermost added last)
    app.add_middleware(
        RedisRateLimiterMiddleware,
        limit_per_minute=rate_limit_per_minute,
        redis_client=redis_client,
        enabled=True,
    )
    app.add_middleware(
        JWTAuthMiddleware,
        secret_key=secret_key,
        algorithm="HS256",
        auth_enabled=True,
    )
    app.add_middleware(CorrelationIdLoggingMiddleware)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": settings.service_name}

    @app.post("/api/v1/transactions", status_code=202)
    async def ingest_transaction(payload: TransactionCreateRequest, request: Request) -> JSONResponse:
        cid = getattr(request.state, "correlation_id", str(uuid.uuid4()))
        resp = TransactionAcceptedResponse(
            event_id=payload.event_id,
            transaction_id=payload.transaction_id,
            status="accepted",
            schema_version=payload.schema_version,
            topic=settings.kafka_transactions_topic,
            partition=2,
            kafka_offset=10482,
            correlation_id=cid,
        )
        return JSONResponse(status_code=202, content=resp.model_dump())

    return app


def test_pydantic_schema_accepts_canonical_and_legacy_aliases() -> None:
    """TC-04: Verifies both canonical TransactionEvent v1.0 fields (`event_id`, `event_time`,
    `category`, `device`) and legacy aliases (`transaction_id`, `timestamp`, `merchant_category`,
    `device_info`) validate cleanly, normalize timestamps to UTC `Z`, and keep optional fields `None`.
    """
    event_uuid = str(uuid.uuid4())
    customer_uuid = str(uuid.uuid4())
    merchant_uuid = str(uuid.uuid4())

    # 1. Canonical v1.0 payload (with +05:30 offset timestamp and omitted optional device)
    canonical_payload = {
        "event_id": event_uuid,
        "event_time": "2026-11-12T20:00:00+05:30",
        "customer_id": customer_uuid,
        "merchant_id": merchant_uuid,
        "amount": 249.95,
        "currency": "usd",
        "category": "Electronics",
        "location": {
            "country": "us",
            "city": "New York",
            "latitude": 40.7128,
            "longitude": -74.0060,
        },
    }
    tx_canonical = TransactionCreateRequest.model_validate(canonical_payload)
    assert tx_canonical.event_id == event_uuid
    assert tx_canonical.transaction_id == event_uuid
    assert tx_canonical.event_time == "2026-11-12T14:30:00Z"
    assert tx_canonical.timestamp == "2026-11-12T14:30:00Z"
    assert tx_canonical.currency == "USD"
    assert tx_canonical.category == "electronics"
    assert tx_canonical.location.country == "US"
    assert tx_canonical.device is None  # Never imputed when omitted
    assert tx_canonical.schema_version == "1.0"

    canonical_dict = tx_canonical.to_canonical_dict()
    assert canonical_dict["event_id"] == event_uuid
    assert canonical_dict["event_time"] == "2026-11-12T14:30:00Z"
    assert canonical_dict["category"] == "electronics"
    assert canonical_dict["device"] is None
    assert canonical_dict["schema_version"] == "1.0"

    # 2. Legacy alias payload (`transaction_id`, `timestamp`, `merchant_category`, `device_info`)
    alias_payload = {
        "transaction_id": event_uuid,
        "timestamp": "2026-11-12T14:30:00.123456Z",
        "customer_id": customer_uuid,
        "merchant_id": merchant_uuid,
        "amount": 89.50,
        "currency": "EUR",
        "merchant_category": "Groceries",
        "location": {
            "country": "GB",
            "city": "London",
            "latitude": 51.5074,
            "longitude": -0.1278,
        },
        "device_info": {
            "device_id": "dev-001",
            "device_type": "mobile",
            "os": "iOS",
            "ip_address": "192.0.2.10",
        },
    }
    tx_alias = TransactionCreateRequest.model_validate(alias_payload)
    assert tx_alias.event_id == event_uuid
    assert tx_alias.event_time == "2026-11-12T14:30:00.123456Z"
    assert tx_alias.category == "groceries"
    assert tx_alias.device is not None
    assert tx_alias.device.device_id == "dev-001"
    assert tx_alias.device.browser is None  # Unprovided optional subfield stays None


def test_pydantic_schema_preserves_high_value_outliers_and_rejects_negative() -> None:
    """TC-04: Verifies $1,000,000.00 high-value outlier transactions are preserved (Lab678_21.pdf
    Section 4) while negative/zero amounts, malformed UUIDs, and out-of-bounds coordinates fail.
    """
    base_payload = {
        "event_id": str(uuid.uuid4()),
        "event_time": "2026-11-12T14:30:00Z",
        "customer_id": str(uuid.uuid4()),
        "merchant_id": str(uuid.uuid4()),
        "amount": 1_000_000.00,
        "currency": "USD",
        "category": "wire_transfer",
        "location": {
            "country": "US",
            "city": "Chicago",
            "latitude": 41.8781,
            "longitude": -87.6298,
        },
    }

    # High-value outlier ($1,000,000.00) must succeed without artificial upper cap
    outlier_tx = TransactionCreateRequest.model_validate(base_payload)
    assert outlier_tx.amount == 1_000_000.00

    # Negative amount (-50.00) and zero (0.0) must be rejected
    for invalid_amount in (-50.00, 0.0):
        bad_amount_payload = {**base_payload, "amount": invalid_amount}
        with pytest.raises(ValidationError):
            TransactionCreateRequest.model_validate(bad_amount_payload)

    # Invalid UUID must be rejected
    bad_uuid_payload = {**base_payload, "customer_id": "not-a-valid-uuid"}
    with pytest.raises(ValidationError):
        TransactionCreateRequest.model_validate(bad_uuid_payload)

    # Out-of-bounds latitude (> 90) must be rejected
    bad_geo_payload = {
        **base_payload,
        "location": {"country": "US", "city": "NYC", "latitude": 125.0, "longitude": -74.0},
    }
    with pytest.raises(ValidationError):
        TransactionCreateRequest.model_validate(bad_geo_payload)

    # Verify DLQErrorResponse model structure
    dlq_resp = DLQErrorResponse(
        error_code="ERR_INVALID_AMOUNT",
        detail="amount must be > 0",
        dlq_routed=True,
        correlation_id="corr-123",
    )
    assert dlq_resp.error_code == "ERR_INVALID_AMOUNT"
    assert dlq_resp.dlq_routed is True


def test_jwt_auth_rejects_missing_or_invalid_token() -> None:
    """TC-05: Verifies HTTP 401 on missing/invalid/expired JWT, public path bypass on /health,
    and HTTP 202 with X-Correlation-ID propagation when a valid Bearer JWT is supplied.
    """
    secret = "test-jwt-secret-key-hermes-2026"
    app = _build_middleware_test_app(secret_key=secret)
    client = TestClient(app)

    sample_tx = {
        "event_id": str(uuid.uuid4()),
        "event_time": "2026-11-12T14:30:00Z",
        "customer_id": str(uuid.uuid4()),
        "merchant_id": str(uuid.uuid4()),
        "amount": 125.00,
        "currency": "USD",
        "category": "retail",
        "location": {"country": "US", "city": "Seattle", "latitude": 47.6062, "longitude": -122.3321},
    }

    # 1. Public endpoint (/health) bypasses JWT authentication and attaches X-Correlation-ID
    health_resp = client.get("/health", headers={"X-Correlation-ID": "cid-health-001"})
    assert health_resp.status_code == 200
    assert health_resp.headers.get("X-Correlation-ID") == "cid-health-001"

    # 2. Missing Authorization header -> 401 Unauthorized
    no_auth_resp = client.post("/api/v1/transactions", json=sample_tx)
    assert no_auth_resp.status_code == 401
    assert no_auth_resp.json()["error_code"] == "ERR_UNAUTHORIZED"

    # 3. Invalid signature / tampered token -> 401 Unauthorized
    bad_token = create_access_token(subject="client-1", secret_key="wrong-secret-key")
    bad_auth_resp = client.post(
        "/api/v1/transactions",
        json=sample_tx,
        headers={"Authorization": f"Bearer {bad_token}"},
    )
    assert bad_auth_resp.status_code == 401
    assert bad_auth_resp.json()["error_code"] == "ERR_UNAUTHORIZED"

    # 4. Expired token -> 401 Unauthorized
    expired_token = create_access_token(subject="client-1", expires_delta_minutes=-5, secret_key=secret)
    expired_resp = client.post(
        "/api/v1/transactions",
        json=sample_tx,
        headers={"Authorization": f"Bearer {expired_token}"},
    )
    assert expired_resp.status_code == 401
    assert "expired" in expired_resp.json()["detail"].lower()

    # 5. Valid JWT token -> 202 Accepted with correlation_id propagated
    valid_token = create_access_token(subject="simulator-client", secret_key=secret)
    valid_resp = client.post(
        "/api/v1/transactions",
        json=sample_tx,
        headers={
            "Authorization": f"Bearer {valid_token}",
            "X-Correlation-ID": "corr-valid-777",
        },
    )
    assert valid_resp.status_code == 202
    body = valid_resp.json()
    assert body["event_id"] == sample_tx["event_id"]
    assert body["status"] == "accepted"
    assert body["correlation_id"] == "corr-valid-777"
    assert valid_resp.headers.get("X-Correlation-ID") == "corr-valid-777"


class _FakeRedisPipeline:
    def __init__(self, store: dict[str, int]) -> None:
        self._store = store
        self._ops: list[tuple[str, str]] = []

    def incr(self, key: str) -> "_FakeRedisPipeline":
        self._ops.append(("incr", key))
        return self

    def expire(self, key: str, seconds: int) -> "_FakeRedisPipeline":
        self._ops.append(("expire", key))
        return self

    def execute(self) -> list[Any]:
        results: list[Any] = []
        for op, key in self._ops:
            if op == "incr":
                self._store[key] = self._store.get(key, 0) + 1
                results.append(self._store[key])
            elif op == "expire":
                results.append(True)
        return results


class _FakeRedisClient:
    def __init__(self) -> None:
        self.store: dict[str, int] = {}

    def pipeline(self) -> _FakeRedisPipeline:
        return _FakeRedisPipeline(self.store)


def test_rate_limiter_enforces_429_above_threshold() -> None:
    """TC-05: Sets rate limit to 5 req/min in test fixture and verifies requests 1..5 succeed
    while the 6th request returns HTTP 429 with Retry-After: 60.
    """
    secret = "test-jwt-secret-key-hermes-2026"
    fake_redis = _FakeRedisClient()
    app = _build_middleware_test_app(
        rate_limit_per_minute=5,
        redis_client=fake_redis,
        secret_key=secret,
    )
    client = TestClient(app)
    token = create_access_token(subject="load-client", secret_key=secret)

    sample_tx = {
        "event_id": str(uuid.uuid4()),
        "event_time": "2026-11-12T14:30:00Z",
        "customer_id": str(uuid.uuid4()),
        "merchant_id": str(uuid.uuid4()),
        "amount": 42.00,
        "currency": "USD",
        "category": "dining",
        "location": {"country": "US", "city": "Austin", "latitude": 30.2672, "longitude": -97.7431},
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Client-ID": "client-rate-test-01",
    }

    # First 5 requests within the minute bucket must succeed (202 Accepted)
    for i in range(5):
        resp = client.post("/api/v1/transactions", json=sample_tx, headers=headers)
        assert resp.status_code == 202, f"Request {i + 1} failed unexpectedly"
        assert resp.headers.get("X-RateLimit-Limit") == "5"

    # 6th request must be rate-limited with 429 Too Many Requests and Retry-After: 60
    sixth_resp = client.post("/api/v1/transactions", json=sample_tx, headers=headers)
    assert sixth_resp.status_code == 429
    assert sixth_resp.headers.get("Retry-After") == "60"
    assert sixth_resp.json()["error_code"] == "ERR_RATE_LIMIT_EXCEEDED"
