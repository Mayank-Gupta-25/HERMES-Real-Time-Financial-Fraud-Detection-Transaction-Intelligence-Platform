from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any
import uuid

_SERVICE_DIR = Path(__file__).resolve().parents[1]
if str(_SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(_SERVICE_DIR))
if "app" in sys.modules and not str(getattr(sys.modules["app"], "__file__", "")).startswith(
    str(_SERVICE_DIR)
):
    for mod_name in [k for k in sys.modules if k == "app" or k.startswith("app.")]:
        del sys.modules[mod_name]

from app.engine.scorer import HybridFraudScorer  # noqa: E402
from app.engine.velocity_checker import VelocityChecker  # noqa: E402


REQUIRED_FRAUD_DECISION_KEYS = {
    "event_id",
    "transaction_id",
    "event_time",
    "customer_id",
    "merchant_id",
    "amount",
    "currency",
    "channel",
    "category",
    "location",
    "fraud_score",
    "rule_score",
    "ml_score",
    "stat_score",
    "z_score_signal",
    "risk_band",
    "risk_level",
    "decision",
    "triggered_rules",
    "reasons",
    "model_version",
    "shadow_model_version",
    "shadow_ml_score",
    "rule_version",
    "transformation_version",
    "schema_version",
    "feature_time",
    "decision_time",
    "processing_time_ms",
    "feature_values",
    "provenance",
}


def test_hybrid_score_band_and_decision_mapping() -> None:
    """
    TC-17: Verify exact risk band (`low`, `medium`, `high`, `critical`) and
    decision (`approve`, `flag`, `block`) boundaries (`0.40`, `0.65`, `0.85`).
    """
    scorer = HybridFraudScorer()
    assert scorer.weights["alpha_rule"] == 0.40
    assert scorer.weights["beta_ml"] == 0.45
    assert scorer.weights["gamma_stat"] == 0.15

    # Low: [0.00, 0.40) -> approve
    assert scorer.classify_risk_band_and_decision(0.00) == ("low", "approve")
    assert scorer.classify_risk_band_and_decision(0.3999) == ("low", "approve")

    # Medium: [0.40, 0.65) -> approve
    assert scorer.classify_risk_band_and_decision(0.40) == ("medium", "approve")
    assert scorer.classify_risk_band_and_decision(0.6499) == ("medium", "approve")

    # High: [0.65, 0.85) -> flag
    assert scorer.classify_risk_band_and_decision(0.65) == ("high", "flag")
    assert scorer.classify_risk_band_and_decision(0.8499) == ("high", "flag")

    # Critical: [0.85, 1.00] -> block
    assert scorer.classify_risk_band_and_decision(0.85) == ("critical", "block")
    assert scorer.classify_risk_band_and_decision(1.00) == ("critical", "block")

    # Verify weighted linear combination
    combined = scorer.combine_scores(rule_score=0.50, ml_score=0.60, stat_score=0.20)
    # 0.40*0.50 + 0.45*0.60 + 0.15*0.20 = 0.20 + 0.27 + 0.03 = 0.50
    assert combined == 0.50


def test_temporal_leakage_prevention_and_warmup() -> None:
    """
    TC-14 / TC-17: Feed 6 sequential transactions for a brand-new customer.
    Assert transactions 1-5 have `is_warmup == True` and `amount_z_score == 0.0`,
    and transaction 6 uses the mean/std of transactions 1-5 only (zero temporal leakage).
    """
    vel = VelocityChecker(warmup_min_events=5, use_redis=False)
    scorer = HybridFraudScorer(velocity_checker=vel)

    customer_id = str(uuid.uuid4())
    merchant_id = str(uuid.uuid4())
    base_time = datetime(2026, 11, 12, 12, 0, 0, tzinfo=timezone.utc)
    baseline_amounts = [90.0, 95.0, 100.0, 105.0, 110.0]  # mean = 100.0

    for idx, amt in enumerate(baseline_amounts, start=1):
        evt: dict[str, Any] = {
            "event_id": str(uuid.uuid4()),
            "event_time": (base_time + timedelta(minutes=idx * 15)).isoformat().replace("+00:00", "Z"),
            "customer_id": customer_id,
            "merchant_id": merchant_id,
            "amount": amt,
            "currency": "USD",
            "channel": "online",
            "category": "groceries",
            "location": {
                "country": "US",
                "city": "Chicago",
                "latitude": 41.8781,
                "longitude": -87.6298,
            },
            "device": {"device_id": "d-1", "device_type": "mobile"},
        }
        decision = scorer.score(event=evt)
        fv = decision["feature_values"]
        assert fv["prior_count"] == idx - 1
        assert fv["is_warmup"] is True
        assert fv["amount_z_score"] == 0.0
        assert "AMOUNT_SPIKE" not in decision["triggered_rules"]

    # 6th transaction: large $2,500 spike. Prior baseline must reflect ONLY events 1..5 (mean = 100.0)
    evt_6: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_time": (base_time + timedelta(minutes=120)).isoformat().replace("+00:00", "Z"),
        "customer_id": customer_id,
        "merchant_id": merchant_id,
        "amount": 2500.0,
        "currency": "USD",
        "channel": "online",
        "category": "electronics",
        "location": {
            "country": "US",
            "city": "Chicago",
            "latitude": 41.8781,
            "longitude": -87.6298,
        },
        "device": {"device_id": "d-1", "device_type": "mobile"},
    }
    decision_6 = scorer.score(event=evt_6)
    fv_6 = decision_6["feature_values"]

    assert REQUIRED_FRAUD_DECISION_KEYS.issubset(decision_6.keys())
    assert fv_6["prior_count"] == 5
    assert fv_6["is_warmup"] is False
    assert fv_6["amount_mean_prior"] == 100.0
    assert fv_6["amount_z_score"] > 100.0
    assert "AMOUNT_SPIKE" in decision_6["triggered_rules"]


def test_fallback_when_ml_model_unavailable() -> None:
    """
    TC-17: Verify graceful fallback using `fallback_alpha_rule` (0.65) and
    `fallback_gamma_stat` (0.35) when ML model is unavailable.
    """
    scorer = HybridFraudScorer()
    event = {
        "event_id": str(uuid.uuid4()),
        "event_time": "2026-11-12T14:30:00Z",
        "customer_id": str(uuid.uuid4()),
        "merchant_id": str(uuid.uuid4()),
        "amount": 3500.0,
        "currency": "USD",
        "channel": "online",
        "category": "money_transfer",
        "is_international": True,
        "location": {
            "country": "GB",
            "city": "London",
            "latitude": 51.5072,
            "longitude": -0.1276,
        },
    }
    features = {
        "is_warmup": False,
        "prior_count": 10,
        "amount_mean_prior": 100.0,
        "amount_std_prior": 25.0,
        "amount_z_score": 15.0,
        "is_international": 1,
        "device_change_flag": 1,
    }

    decision = scorer.score(event=event, features=features, ml_available=False)
    assert decision["provenance"]["fallback_used"] is True
    assert decision["ml_score"] == 0.0
    assert decision["fraud_score"] >= 0.65
    assert decision["decision"] in ("flag", "block")
    assert "INTERNATIONAL_HIGH_AMOUNT" in decision["triggered_rules"]
    assert "NEW_DEVICE_HIGH_AMOUNT" in decision["triggered_rules"]
