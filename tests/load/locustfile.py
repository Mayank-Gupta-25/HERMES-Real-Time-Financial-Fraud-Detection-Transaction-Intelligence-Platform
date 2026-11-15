"""
HERMES Locust Load Testing Suite (`tests/load/locustfile.py` — `MOD-09-FN-02`, `TC-29`).

Defines two concurrent load user classes:
  1. `HermesIngestionUser`: Authenticates via JWT and hammers `POST /api/v1/transactions`
     with canonical `TransactionEvent` v1.0 normal and fraudulent payloads.
  2. `HermesAnalyticsUser`: Polls `/api/v1/analytics/overview`, `/volume`, and
     `/transactions` to measure concurrent analytical read latency under write load.

Includes a clean stdlib fallback if `locust` is not installed in the host Python
environment so unit and benchmark verification tests can import and exercise the
user classes offline.
"""
from __future__ import annotations

from datetime import datetime, timezone
import random
from typing import Any, Callable, Dict, Optional
import uuid

try:
    from locust import HttpUser, between, task  # type: ignore

    HAS_LOCUST = True
except ImportError:
    HAS_LOCUST = False

    def between(min_wait: float, max_wait: float) -> Callable[..., float]:
        def _wait() -> float:
            return (min_wait + max_wait) / 2.0

        return _wait

    def task(weight: int = 1) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            setattr(fn, "_locust_task_weight", weight)
            return fn

        return _decorator

    class HttpUser:  # type: ignore[no-redef]
        abstract = True
        wait_time = between(0.01, 0.05)

        def __init__(self, client: Optional[Any] = None) -> None:
            self.client = client


CATEGORIES = [
    "groceries",
    "electronics",
    "dining",
    "travel",
    "utilities",
    "jewelry",
    "money_transfer",
    "gambling",
]
CHANNELS = ["online", "pos", "mobile", "atm"]
CITIES = [
    {"country": "US", "city": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"country": "US", "city": "Chicago", "latitude": 41.8781, "longitude": -87.6298},
    {"country": "GB", "city": "London", "latitude": 51.5074, "longitude": -0.1278},
    {"country": "IN", "city": "Mumbai", "latitude": 19.0760, "longitude": 72.8777},
    {"country": "RU", "city": "Moscow", "latitude": 55.7558, "longitude": 37.6173},
]


def build_load_transaction_payload(is_fraud: bool = False) -> Dict[str, Any]:
    """Generate a canonical TransactionEvent v1.0 payload for load testing."""
    eid = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    loc = CITIES[-1] if is_fraud else random.choice(CITIES[:-1])
    amount = round(random.uniform(8200.0, 9850.0), 2) if is_fraud else round(random.uniform(12.0, 350.0), 2)
    category = "money_transfer" if is_fraud else random.choice(CATEGORIES[:5])

    return {
        "event_id": eid,
        "transaction_id": eid,
        "event_time": now_iso,
        "timestamp": now_iso,
        "customer_id": str(uuid.uuid4()),
        "merchant_id": str(uuid.uuid4()),
        "amount": amount,
        "currency": "USD",
        "transaction_type": "transfer" if is_fraud else "purchase",
        "channel": random.choice(CHANNELS),
        "card_type": "credit",
        "category": category,
        "merchant_category": category,
        "location": dict(loc),
        "device": {
            "device_id": f"dev-{uuid.uuid4().hex[:8]}",
            "ip_address": "198.51.100.42",
            "device_type": "mobile",
            "os": "iOS",
        },
        "is_international": bool(is_fraud),
        "status": "pending",
        "schema_version": "1.0",
    }


class HermesIngestionUser(HttpUser):
    """Simulates high-throughput transaction ingestion via API Gateway."""

    wait_time = between(0.001, 0.01)
    token: str = "dev-load-test-token"

    def on_start(self) -> None:
        if self.client is not None and hasattr(self.client, "post"):
            try:
                resp = self.client.post(
                    "/api/v1/auth/token",
                    json={"client_id": "locust-load-runner", "role": "ingestor"},
                )
                if getattr(resp, "status_code", 0) == 200:
                    data = resp.json()
                    self.token = data.get("access_token", self.token)
            except Exception:
                pass

    @task(9)
    def post_normal_transaction(self) -> Any:
        payload = build_load_transaction_payload(is_fraud=False)
        headers = {
            "Authorization": f"Bearer {self.token}",
            "X-Correlation-ID": payload["event_id"],
        }
        if self.client is not None:
            return self.client.post("/api/v1/transactions", json=payload, headers=headers)
        return payload

    @task(1)
    def post_fraud_transaction(self) -> Any:
        payload = build_load_transaction_payload(is_fraud=True)
        headers = {
            "Authorization": f"Bearer {self.token}",
            "X-Correlation-ID": payload["event_id"],
        }
        if self.client is not None:
            return self.client.post("/api/v1/transactions", json=payload, headers=headers)
        return payload


class HermesAnalyticsUser(HttpUser):
    """Simulates concurrent dashboard operators querying the Analytics Service."""

    wait_time = between(0.05, 0.25)

    @task(4)
    def get_overview_kpis(self) -> Any:
        if self.client is not None:
            return self.client.get("/api/v1/analytics/overview?range=24h")
        return {"endpoint": "/api/v1/analytics/overview"}

    @task(3)
    def get_volume_timeseries(self) -> Any:
        if self.client is not None:
            return self.client.get("/api/v1/analytics/volume?interval=5m&range=24h")
        return {"endpoint": "/api/v1/analytics/volume"}

    @task(3)
    def get_transactions_page(self) -> Any:
        if self.client is not None:
            return self.client.get("/api/v1/analytics/transactions?page=1&page_size=20")
        return {"endpoint": "/api/v1/analytics/transactions"}
