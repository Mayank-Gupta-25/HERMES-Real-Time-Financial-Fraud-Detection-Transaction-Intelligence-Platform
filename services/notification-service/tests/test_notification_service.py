from __future__ import annotations

from pathlib import Path
import sys
import time
import uuid

from fastapi.testclient import TestClient

_SERVICE_DIR = Path(__file__).resolve().parents[1]
if str(_SERVICE_DIR) in sys.path:
    sys.path.remove(str(_SERVICE_DIR))
sys.path.insert(0, str(_SERVICE_DIR))
if "app" in sys.modules and not str(getattr(sys.modules["app"], "__file__", "")).startswith(
    str(_SERVICE_DIR)
):
    for mod_name in [k for k in list(sys.modules) if k == "app" or k.startswith("app.")]:
        del sys.modules[mod_name]

from app.main import app  # noqa: E402


def test_websocket_broadcasts_fraud_alert_and_deduplicates() -> None:
    """
    TC-19: Connect via FastAPI `TestClient.websocket_connect("/ws/alerts")`,
    publish a `FRAUD_ALERT` event, and verify immediate receipt (< 100ms),
    Section 10.F.1 schema compliance, and duplicate suppression by `alert_id`/`event_id`.
    """
    client = TestClient(app)
    app.state.ws_manager.reset()
    app.state.alert_consumer.processed_count = 0
    app.state.alert_consumer.duplicate_count = 0

    event_id = "550e8400-e29b-41d4-a716-446655440000"
    alert_payload = {
        "type": "FRAUD_ALERT",
        "alert_id": f"alert-{event_id}",
        "event_id": event_id,
        "transaction_id": event_id,
        "event_time": "2026-11-12T14:30:00.120Z",
        "customer_id": "c1111111-e29b-41d4-a716-446655440001",
        "merchant_id": "m2222222-e29b-41d4-a716-446655440002",
        "amount": 9450.00,
        "currency": "USD",
        "category": "money_transfer",
        "fraud_score": 0.9125,
        "risk_band": "critical",
        "risk_level": "critical",
        "decision": "block",
        "triggered_rules": ["GEO_IMPOSSIBILITY", "INTERNATIONAL_HIGH_AMOUNT"],
        "status": "active",
    }

    with client.websocket_connect("/ws/alerts") as ws:
        t0 = time.perf_counter()
        resp = client.post("/api/v1/notify/test", json=alert_payload)
        assert resp.status_code == 200
        body = resp.json()
        assert body["dispatched"] is True
        assert body["duplicate"] is False
        assert body["email_sent"] is True
        assert body["webhook_sent"] is True

        received = ws.receive_json()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        assert elapsed_ms < 100.0

        assert received["type"] == "FRAUD_ALERT"
        assert received["alert_id"] == f"alert-{event_id}"
        assert received["event_id"] == event_id
        assert received["risk_band"] == "critical"
        assert received["decision"] == "block"
        assert "GEO_IMPOSSIBILITY" in received["triggered_rules"]

        # Re-publishing the exact same alert_id/event_id must be suppressed as a duplicate
        dup_resp = client.post("/api/v1/notify/test", json=alert_payload)
        assert dup_resp.status_code == 200
        dup_body = dup_resp.json()
        assert dup_body["dispatched"] is False
        assert dup_body["duplicate"] is True

        # Send a second unique alert and confirm the first duplicate did NOT queue a message
        second_event_id = str(uuid.uuid4())
        second_alert = {
            **alert_payload,
            "alert_id": f"alert-{second_event_id}",
            "event_id": second_event_id,
            "amount": 3200.00,
            "risk_band": "high",
            "decision": "flag",
        }
        resp2 = client.post("/api/v1/notify/test", json=second_alert)
        assert resp2.status_code == 200
        received2 = ws.receive_json()
        assert received2["event_id"] == second_event_id
        assert received2["risk_band"] == "high"

    recent_resp = client.get("/api/v1/alerts/recent")
    assert recent_resp.status_code == 200
    recent_data = recent_resp.json()
    assert recent_data["count"] == 2
    assert recent_data["broadcast_count"] == 2
    assert recent_data["duplicate_dropped_count"] == 1
