"""
Integration tests for ML Ensemble Artifact Loading, Validation-Calibrated Hybrid
Weight Freezing (`scoring_weights.json`), and `< 50 ms` End-to-End Inference
inside `services/fraud-detection/` (`STEP AAKARSH-MOD04-02`, `TC-16`, `TC-17`).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time
import uuid

from fastapi.testclient import TestClient
import pytest

_SERVICE_DIR = Path(__file__).resolve().parents[1]
_REPO_ROOT = _SERVICE_DIR.parent.parent

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if str(_SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(_SERVICE_DIR))
if "tests" in sys.modules and hasattr(sys.modules["tests"], "__path__"):
    for _extra_tests_dir in [
        _REPO_ROOT / "services" / "transaction-processor" / "tests",
        _REPO_ROOT / "services" / "transaction-simulator" / "tests",
        _REPO_ROOT / "ml" / "tests",
        _REPO_ROOT / "tests",
    ]:
        _extra_str = str(_extra_tests_dir)
        if _extra_tests_dir.exists() and _extra_str not in sys.modules["tests"].__path__:
            sys.modules["tests"].__path__.append(_extra_str)
if "app" in sys.modules and not str(getattr(sys.modules["app"], "__file__", "")).startswith(
    str(_SERVICE_DIR)
):
    for mod_name in [k for k in sys.modules if k == "app" or k.startswith("app.")]:
        del sys.modules[mod_name]

from app.engine.scorer import HybridFraudScorer  # noqa: E402
from app.main import app  # noqa: E402
from app.models.ensemble import EnsembleModelWrapper  # noqa: E402
from ml.src.tune_hybrid_weights import DEFAULT_WEIGHTS_PATH, calibrate_and_freeze_weights  # noqa: E402


def test_hybrid_weight_calibration_freezes_valid_simplex() -> None:
    """TC-16 / TC-17: Verify validation-split weight calibration freezes alpha + beta + gamma = 1.0."""
    calibrated = calibrate_and_freeze_weights(n_samples=2000, seed=42)
    alpha = float(calibrated["alpha_rule"])
    beta = float(calibrated["beta_ml"])
    gamma = float(calibrated["gamma_stat"])

    assert round(alpha + beta + gamma, 4) == pytest.approx(1.0)
    assert alpha == pytest.approx(0.40)
    assert beta == pytest.approx(0.45)
    assert gamma == pytest.approx(0.15)
    assert "calibration_summary" in calibrated
    assert calibrated["calibration_summary"]["validation_metrics"]["recall"] >= 0.85

    with open(DEFAULT_WEIGHTS_PATH, "r", encoding="utf-8") as fh:
        saved = json.load(fh)
    assert saved["weights_version"] == "weights-v1.0"
    assert round(saved["alpha_rule"] + saved["beta_ml"] + saved["gamma_stat"], 4) == pytest.approx(1.0)


def test_ensemble_wrapper_loads_trained_joblib_artifacts_under_15ms() -> None:
    """TC-16: Verify `EnsembleModelWrapper` loads `.joblib` artifacts from `ml/models/v1/` and runs in < 15 ms."""
    models_dir = _REPO_ROOT / "ml" / "models" / "v1"
    ensemble = EnsembleModelWrapper(model_path=models_dir)

    assert ensemble.artifacts_loaded is True
    assert ensemble.xgb_wrapper.is_loaded is True
    assert ensemble.if_wrapper.is_loaded is True
    assert ensemble.metrics.get("auc_roc", 0.0) >= 0.90

    sample_features = {
        "txn_count_1m": 6,
        "txn_count_5m": 8,
        "txn_count_1h": 11,
        "amount_sum_1h": 15400.0,
        "amount_mean_prior": 82.0,
        "amount_std_prior": 18.5,
        "amount_z_score": 140.2,
        "time_since_last_sec": 3.5,
        "geo_distance_km": 6800.0,
        "travel_speed_kmph": 5200.0,
        "is_international": 1,
        "merchant_category_freq": 0.0,
        "hour_of_day": 3,
        "day_of_week": 6,
        "is_weekend": 1,
        "device_change_flag": 1,
        "is_warmup": False,
    }

    # Warm up once to avoid cold import/threading overhead
    _ = ensemble.predict(sample_features)

    t0 = time.perf_counter()
    pred = ensemble.predict(sample_features)
    latency_ms = (time.perf_counter() - t0) * 1000.0

    assert pred["available"] is True
    assert pred["artifacts_loaded"] is True
    assert 0.0 <= pred["ml_score"] <= 1.0
    assert pred["ml_score"] >= 0.65
    assert latency_ms < 15.0, f"Expected ensemble inference < 15 ms, got {latency_ms:.2f} ms"


def test_end_to_end_post_score_combines_rule_ml_and_stat_with_frozen_weights() -> None:
    """TC-17: Verify `POST /score` combines Rule + ML + Stat scores using frozen weights in < 50 ms."""
    with TestClient(app) as client:
        scorer: HybridFraudScorer = app.state.scorer
        scorer.reload_weights()
        scorer.ensemble_model.reload_models()

        req = {
            "event": {
                "event_id": str(uuid.uuid4()),
                "event_time": "2026-11-12T16:20:00Z",
                "customer_id": str(uuid.uuid4()),
                "merchant_id": str(uuid.uuid4()),
                "amount": 6400.0,
                "currency": "USD",
                "channel": "online",
                "category": "jewelry",
                "is_international": True,
                "location": {
                    "country": "RU",
                    "city": "Moscow",
                    "latitude": 55.7558,
                    "longitude": 37.6173,
                },
                "device": {"device_id": "dev-x", "device_type": "tablet"},
                "schema_version": "1.0",
            },
            "features": {
                "txn_count_1m": 4,
                "txn_count_5m": 6,
                "txn_count_1h": 8,
                "amount_sum_1h": 18200.0,
                "amount_mean_prior": 90.0,
                "amount_std_prior": 20.0,
                "amount_z_score": 315.5,
                "time_since_last_sec": 12.0,
                "geo_distance_km": 7400.0,
                "travel_speed_kmph": 4200.0,
                "is_international": 1,
                "merchant_category_freq": 0.0,
                "hour_of_day": 2,
                "day_of_week": 6,
                "is_weekend": 1,
                "device_change_flag": 1,
                "prior_count": 12,
                "is_warmup": False,
            },
        }

        # Warm up once
        _ = client.post("/score", json=req)

        t0 = time.perf_counter()
        resp = client.post("/score", json=req)
        duration_ms = (time.perf_counter() - t0) * 1000.0

        assert resp.status_code == 200
        body = resp.json()

        assert body["decision"] == "block"
        assert body["risk_band"] == "critical"
        assert body["fraud_score"] >= 0.85
        assert body["ml_score"] > 0.50
        assert body["rule_score"] >= 0.65
        assert body["stat_score"] > 0.50
        assert body["processing_time_ms"] < 50.0
        assert duration_ms < 50.0
        assert body["provenance"]["weights_version"] == "weights-v1.0"
        assert body["provenance"]["fallback_used"] is False
