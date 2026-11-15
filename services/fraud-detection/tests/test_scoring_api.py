from __future__ import annotations

from pathlib import Path
import sys
import uuid

from fastapi.testclient import TestClient

_SERVICE_DIR = Path(__file__).resolve().parents[1]
if str(_SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(_SERVICE_DIR))
if "app" in sys.modules and not str(getattr(sys.modules["app"], "__file__", "")).startswith(
    str(_SERVICE_DIR)
):
    for mod_name in [k for k in sys.modules if k == "app" or k.startswith("app.")]:
        del sys.modules[mod_name]

from app.main import app  # noqa: E402


client = TestClient(app)


def test_post_score_returns_complete_fraud_decision_under_50ms() -> None:
    """
    TC-17 / TC-31: Send a high-risk geo-impossible transaction to `POST /score`,
    verify `200 OK`, `decision in ("flag", "block")`, `GEO_IMPOSSIBILITY` in `triggered_rules`,
    all Section 10.B provenance/audit fields present, and `processing_time_ms < 50`.
    """
    # Route / framework warmup
    client.post("/score", json={"event": {"event_id": str(uuid.uuid4()), "amount": 10.0}})

    payload = {
        "event": {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "event_time": "2026-11-12T14:30:00.120Z",
            "customer_id": "c1111111-e29b-41d4-a716-446655440001",
            "merchant_id": "m2222222-e29b-41d4-a716-446655440002",
            "amount": 9450.00,
            "currency": "USD",
            "channel": "online",
            "category": "money_transfer",
            "is_international": True,
            "location": {
                "country": "RU",
                "city": "Moscow",
                "latitude": 55.7558,
                "longitude": 37.6173,
            },
            "device": {
                "device_id": "dev-999",
                "device_type": "desktop",
                "os": "Windows",
            },
            "schema_version": "1.0",
        },
        "features": {
            "txn_count_1m": 3,
            "txn_count_5m": 4,
            "txn_count_1h": 6,
            "amount_sum_1h": 14200.0,
            "amount_mean_prior": 68.5,
            "amount_std_prior": 24.1,
            "amount_z_score": 389.27,
            "time_since_last_sec": 180.0,
            "geo_distance_km": 7510.4,
            "travel_speed_kmph": 4820.5,
            "is_international": 1,
            "merchant_category_freq": 0.02,
            "hour_of_day": 14,
            "day_of_week": 3,
            "is_weekend": 0,
            "device_change_flag": 1,
            "prior_count": 15,
            "is_warmup": False,
        },
        "provenance": {
            "source_topic": "hermes.transactions.raw",
            "partition": 2,
            "offset": 10482,
            "ingestion_time": "2026-11-12T14:30:00.125Z",
        },
    }

    response = client.post("/score", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["event_id"] == "550e8400-e29b-41d4-a716-446655440000"
    assert data["transaction_id"] == "550e8400-e29b-41d4-a716-446655440000"
    assert data["decision"] in ("flag", "block")
    assert data["risk_band"] in ("high", "critical")
    assert data["risk_level"] == data["risk_band"]
    assert data["fraud_score"] >= 0.85
    assert "GEO_IMPOSSIBILITY" in data["triggered_rules"]
    assert "INTERNATIONAL_HIGH_AMOUNT" in data["triggered_rules"]
    assert data["rule_version"] == "rules-v1.0"
    assert data["model_version"] == "ensemble-v1.0.0"
    assert data["processing_time_ms"] < 50.0

    # Verify full provenance & feature_values
    assert "provenance" in data
    assert data["provenance"]["source_topic"] == "hermes.transactions.raw"
    assert data["provenance"]["partition"] == 2
    assert data["provenance"]["offset"] == 10482
    assert "postgres:transactions" in data["provenance"]["storage_destinations"]
    assert "postgres:fraud_alerts" in data["provenance"]["storage_destinations"]
    assert "feature_values" in data
    assert data["feature_values"]["travel_speed_kmph"] == 4820.5


def test_post_score_batch_and_clean_approval() -> None:
    """TC-17: Verify `POST /score/batch` scores multiple transactions accurately."""
    batch_payload = {
        "events": [
            {
                "event": {
                    "event_id": str(uuid.uuid4()),
                    "event_time": "2026-11-12T15:00:00Z",
                    "customer_id": str(uuid.uuid4()),
                    "merchant_id": str(uuid.uuid4()),
                    "amount": 24.99,
                    "currency": "USD",
                    "channel": "pos",
                    "category": "groceries",
                    "location": {
                        "country": "US",
                        "city": "Austin",
                        "latitude": 30.2672,
                        "longitude": -97.7431,
                    },
                }
            },
            {
                "event": {
                    "event_id": str(uuid.uuid4()),
                    "event_time": "2026-11-12T15:01:00Z",
                    "customer_id": str(uuid.uuid4()),
                    "merchant_id": str(uuid.uuid4()),
                    "amount": 9200.00,
                    "currency": "USD",
                    "channel": "online",
                    "category": "money_transfer",
                    "location": {
                        "country": "US",
                        "city": "Austin",
                        "latitude": 30.2672,
                        "longitude": -97.7431,
                    },
                },
                "features": {
                    "structuring_txn_count_24h": 3,
                    "amount_sum_1h": 27600.0,
                    "is_warmup": False,
                    "prior_count": 8,
                },
            },
        ]
    }

    resp = client.post("/score/batch", json=batch_payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 2
    assert len(body["decisions"]) == 2
    assert body["decisions"][0]["decision"] == "approve"
    assert body["decisions"][0]["risk_band"] == "low"
    assert body["decisions"][1]["decision"] == "block"
    assert body["decisions"][1]["risk_band"] == "critical"
    assert "STRUCTURING" in body["decisions"][1]["triggered_rules"]


def test_get_rules_reload_model_info_and_health() -> None:
    """TC-17 / TC-31: Verify `GET /rules`, `POST /rules/reload`, `GET /model/info`, `/health`, and `/metrics`."""
    rules_resp = client.get("/rules")
    assert rules_resp.status_code == 200
    rules_data = rules_resp.json()
    assert rules_data["rule_version"] == "rules-v1.0"
    assert rules_data["total_rules"] == 10
    assert len(rules_data["rules"]) == 10

    reload_resp = client.post("/rules/reload")
    assert reload_resp.status_code == 200
    assert reload_resp.json()["reloaded"] is True
    assert reload_resp.json()["rule_count"] == 10

    model_resp = client.get("/model/info")
    assert model_resp.status_code == 200
    model_data = model_resp.json()
    assert model_data["model_version"] == "ensemble-v1.0.0"
    assert model_data["rule_version"] == "rules-v1.0"
    assert model_data["feature_count"] == 16
    assert model_data["weights"]["alpha_rule"] == 0.40
    assert model_data["weights"]["beta_ml"] == 0.45
    assert model_data["weights"]["gamma_stat"] == 0.15
    assert model_data["weights"]["warmup_min_events"] == 5

    health_resp = client.get("/health")
    assert health_resp.status_code == 200
    assert health_resp.json()["status"] == "ok"

    metrics_resp = client.get("/metrics")
    assert metrics_resp.status_code == 200
    assert "hermes_scoring_latency_ms" in metrics_resp.text
