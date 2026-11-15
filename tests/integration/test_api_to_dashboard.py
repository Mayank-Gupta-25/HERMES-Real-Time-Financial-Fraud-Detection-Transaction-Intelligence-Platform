"""
End-to-End Integration Test: API Gateway -> Stream Processing Pipeline -> Storage -> Analytics Service.
Test ID: TC-28 (End-to-End Flow from API to Analytics & Dashboard Verification)
Owner: Aarussh
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any, Dict
import uuid

import pytest
from starlette.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[2]
API_GW_ROOT = REPO_ROOT / "services" / "api-gateway"
PROCESSOR_ROOT = REPO_ROOT / "services" / "transaction-processor"
ANALYTICS_ROOT = REPO_ROOT / "services" / "analytics-service"

for p in (str(API_GW_ROOT), str(PROCESSOR_ROOT), str(ANALYTICS_ROOT), str(REPO_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from services.api_gateway.app.config import settings as gw_settings
from services.api_gateway.app.main import app as gateway_app
from services.api_gateway.app.middleware.auth import create_access_token
from services.transaction_processor.app.config import settings as proc_settings
from services.transaction_processor.app.main import TransactionStreamPipeline
from services.analytics_service.app.db.postgres import get_seed_dataset
from services.analytics_service.app.main import app as analytics_app


def test_end_to_end_api_to_analytics_pipeline(tmp_path: Path) -> None:
    """
    TC-28:
    1. Authenticates against API Gateway with JWT.
    2. Submits a transaction to `POST /api/v1/transactions` and asserts HTTP 202 Accepted.
    3. Runs the transaction through the stream processing pipeline (validation, deduplication,
       enrichment, 16-feature extraction, fraud scoring, polyglot sinks).
    4. Connects to `analytics-service` and queries `/api/v1/analytics/transactions`,
       `/api/v1/analytics/alerts`, and `/api/v1/analytics/overview`.
    5. Asserts full provenance integrity across the pipeline (event_id, model_version,
       rule_version, feature_values, provenance.source_topic).
    """
    gw_client = TestClient(gateway_app)
    analytics_client = TestClient(analytics_app)

    # 1. Generate JWT for ingestion client
    token = create_access_token(
        subject="test-partner-bank-01",
        role="ingest_client",
        expires_delta_minutes=60,
    )
    auth_headers = {
        "Authorization": f"Bearer {token}",
        "X-Correlation-ID": f"corr-{uuid.uuid4()}",
    }

    # 2. Submit high-value geo-impossible international transaction to API Gateway
    target_event_id = str(uuid.uuid4())
    customer_id = "c1111111-e29b-41d4-a716-446655440001"
    merchant_id = "a2222222-e29b-41d4-a716-446655440002"
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    payload = {
        "event_id": target_event_id,
        "event_time": now_iso,
        "customer_id": customer_id,
        "merchant_id": merchant_id,
        "amount": 9500.0,
        "currency": "USD",
        "transaction_type": "purchase",
        "channel": "online",
        "card_type": "credit",
        "category": "money_transfer",
        "location": {
            "country": "RU",
            "city": "Moscow",
            "latitude": 55.7558,
            "longitude": 37.6173,
        },
        "device": {
            "device_id": "dev-international",
            "ip_address": "198.51.100.99",
            "device_type": "mobile",
            "os": "Android",
        },
        "is_international": True,
        "status": "pending",
        "schema_version": "1.0",
    }

    # Verify Gateway accepts valid request
    gw_resp = gw_client.post("/api/v1/transactions", json=payload, headers=auth_headers)
    assert gw_resp.status_code == 202, f"Gateway rejected: {gw_resp.text}"
    gw_data = gw_resp.json()
    assert gw_data["status"] == "accepted"
    assert gw_data["event_id"] == target_event_id
    assert gw_data["topic"] == gw_settings.kafka_transactions_topic
    assert "correlation_id" in gw_data

    # 3. Stream through processing pipeline
    pipeline = TransactionStreamPipeline(checkpoint_dir=tmp_path / "checkpoints")

    # Seed customer profile in Redis lookup
    pipeline.redis_lookup.seed_customer_profile(
        customer_id,
        {
            "home_country": "US",
            "home_city": "New York",
            "home_latitude": 40.7128,
            "home_longitude": -74.0060,
            "risk_tier": "medium",
            "avg_transaction_amount": 120.0,
            "card_type": "credit",
        },
    )

    # Process through pipeline
    decision_rec, dlq = pipeline.process_raw_event(
        raw_payload=payload,
        partition=gw_data.get("partition", 0),
        offset=gw_data.get("kafka_offset", 101),
    )

    assert dlq is None
    assert decision_rec is not None
    assert decision_rec["event_id"] == target_event_id
    assert decision_rec["decision"] in ("flag", "block")
    assert decision_rec["risk_band"] in ("high", "critical")
    assert "GEO_IMPOSSIBILITY" in decision_rec["triggered_rules"] or "INTERNATIONAL_HIGH_AMOUNT" in decision_rec["triggered_rules"]

    # 4. Sync persisted decision and alert into Analytics seed dataset for query verification
    dataset = get_seed_dataset()
    stored_tx = pipeline.postgres_sink.get_transaction(target_event_id)
    assert stored_tx is not None
    dataset["transactions"].insert(0, stored_tx)

    stored_alert = pipeline.postgres_sink.get_alert_by_event_id(target_event_id)
    assert stored_alert is not None
    dataset["fraud_alerts"].insert(0, stored_alert)

    # 5. Query Analytics Service endpoints (Dashboard backend)
    # A. Query transactions list
    tx_resp = analytics_client.get(f"/api/v1/analytics/transactions?customer_id={customer_id}")
    assert tx_resp.status_code == 200
    tx_data = tx_resp.json()
    assert "data" in tx_data
    assert "meta" in tx_data
    assert tx_data["meta"]["source_store"] in ("postgres", "postgresql")
    assert any(t["event_id"] == target_event_id for t in tx_data["data"]["items"])

    matched_tx = next(t for t in tx_data["data"]["items"] if t["event_id"] == target_event_id)
    assert matched_tx["amount"] == 9500.0
    assert matched_tx["decision"] in ("flag", "block")
    assert matched_tx["risk_band"] in ("high", "critical")
    assert matched_tx["model_version"] == "xgboost-v1.0.0"
    assert matched_tx["rule_version"] == "rules-v1.0.0"
    assert matched_tx["schema_version"] == "1.0"
    assert "feature_values" in matched_tx
    assert matched_tx["source_topic"] == proc_settings.topic_transactions_raw

    # B. Query alerts list
    alerts_resp = analytics_client.get("/api/v1/analytics/alerts?status=active")
    assert alerts_resp.status_code == 200
    alerts_data = alerts_resp.json()
    assert any(a["event_id"] == target_event_id for a in alerts_data["data"])

    # C. Query overview KPI metrics
    ov_resp = analytics_client.get("/api/v1/analytics/overview")
    assert ov_resp.status_code == 200
    ov_data = ov_resp.json()
    assert ov_data["data"]["total_transactions"] >= 1
    assert ov_data["data"]["active_alerts_count"] >= 1
    assert ov_data["meta"]["freshness_sla_seconds"] == 5
    assert ov_data["meta"]["consistency_model"] == "bounded_time_range_eventual"
