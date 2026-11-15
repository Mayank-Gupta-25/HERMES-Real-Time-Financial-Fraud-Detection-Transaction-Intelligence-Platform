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

from app.engine.ab_router import ABModelRouter  # noqa: E402
from app.engine.scorer import HybridFraudScorer  # noqa: E402
from app.main import app  # noqa: E402


def test_shadow_router_logs_comparison_without_altering_champion_decision() -> None:
    """
    TC-18: Verify `ABModelRouter` evaluates challenger/shadow model, logs score delta
    and decision divergence/agreement, and NEVER alters the champion `FraudDecision`.
    """
    scorer = HybridFraudScorer()
    ab_router = ABModelRouter(
        champion_version="ensemble-v1.0.0",
        shadow_version="v1.1.0-shadow",
        shadow_sample_rate=1.0,
        shadow_flag_threshold=0.45,  # Intentionally lower flag threshold on shadow to induce divergence
    )

    # 1. Clean event where both champion and shadow agree on approve
    clean_event = {
        "event_id": str(uuid.uuid4()),
        "event_time": "2026-11-12T10:00:00Z",
        "customer_id": str(uuid.uuid4()),
        "merchant_id": str(uuid.uuid4()),
        "amount": 35.0,
        "currency": "USD",
        "channel": "pos",
        "category": "groceries",
        "location": {"country": "US", "city": "Boston", "latitude": 42.3601, "longitude": -71.0589},
    }
    champion_decision_1 = scorer.score(event=clean_event)
    snapshot_1 = dict(champion_decision_1)

    comp_1 = ab_router.evaluate_shadow(champion_decision_1)
    assert comp_1["sampled"] is True
    assert champion_decision_1["decision"] == snapshot_1["decision"] == "approve"
    assert champion_decision_1["fraud_score"] == snapshot_1["fraud_score"]
    assert champion_decision_1["risk_band"] == snapshot_1["risk_band"]

    # 2. Borderline medium-risk event where shadow flags (`>= 0.45`) while champion approves (`< 0.65`)
    borderline_decision = {
        "event_id": str(uuid.uuid4()),
        "fraud_score": 0.52,
        "rule_score": 0.55,
        "ml_score": 0.50,
        "stat_score": 0.45,
        "shadow_ml_score": 0.65,
        "risk_band": "medium",
        "decision": "approve",
        "feature_values": {"txn_count_5m": 4, "amount_z_score": 2.8, "is_warmup": False},
    }
    snapshot_2 = dict(borderline_decision)

    comp_2 = ab_router.evaluate_shadow(borderline_decision)
    assert comp_2["sampled"] is True
    assert comp_2["champion_decision"] == "approve"
    assert comp_2["shadow_decision"] == "flag"
    assert comp_2["decision_agreement"] is False
    assert comp_2["score_delta"] >= 0.0
    # Champion decision must remain untouched!
    assert borderline_decision["decision"] == snapshot_2["decision"] == "approve"
    assert borderline_decision["fraud_score"] == snapshot_2["fraud_score"] == 0.52

    summary = ab_router.get_summary_metrics()
    assert summary["total_requests"] == 2
    assert summary["shadow_evaluated_count"] == 2
    assert summary["decision_agreement_count"] == 1
    assert summary["decision_divergence_count"] == 1
    assert summary["agreement_rate"] == 0.5


def test_ab_router_metrics_exposed_on_model_info_endpoint() -> None:
    """TC-18: Verify `/score` updates `ABModelRouter` metrics exposed by `GET /model/info`."""
    client = TestClient(app)
    app.state.ab_router.reset_metrics()

    resp = client.post(
        "/score",
        json={
            "event": {
                "event_id": str(uuid.uuid4()),
                "event_time": "2026-11-12T12:00:00Z",
                "customer_id": str(uuid.uuid4()),
                "merchant_id": str(uuid.uuid4()),
                "amount": 55.0,
                "currency": "USD",
                "channel": "online",
                "category": "retail",
                "location": {"country": "US", "city": "Seattle", "latitude": 47.6062, "longitude": -122.3321},
            }
        },
    )
    assert resp.status_code == 200

    info_resp = client.get("/model/info")
    assert info_resp.status_code == 200
    info_body = info_resp.json()
    assert "shadow_evaluation" in info_body
    shadow_eval = info_body["shadow_evaluation"]
    assert shadow_eval["shadow_evaluated_count"] >= 1
    assert 0.0 <= shadow_eval["agreement_rate"] <= 1.0
