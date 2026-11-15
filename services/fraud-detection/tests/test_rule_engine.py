from __future__ import annotations

from pathlib import Path
import sys
from typing import Any

import pytest

_SERVICE_DIR = Path(__file__).resolve().parents[1]
if str(_SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(_SERVICE_DIR))
if "app" in sys.modules and not str(getattr(sys.modules["app"], "__file__", "")).startswith(
    str(_SERVICE_DIR)
):
    for mod_name in [k for k in sys.modules if k == "app" or k.startswith("app.")]:
        del sys.modules[mod_name]

from app.engine.rule_engine import RuleEngine  # noqa: E402


def _base_event(**overrides: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "event_time": "2026-11-12T14:30:00Z",
        "customer_id": "c1111111-e29b-41d4-a716-446655440001",
        "merchant_id": "m2222222-e29b-41d4-a716-446655440002",
        "amount": 42.50,
        "currency": "USD",
        "channel": "pos",
        "category": "groceries",
        "location": {
            "country": "US",
            "city": "New York",
            "latitude": 40.7128,
            "longitude": -74.0060,
        },
        "device": {
            "device_id": "dev-100",
            "device_type": "mobile",
            "os": "iOS",
        },
        "is_international": False,
        "schema_version": "1.0",
    }
    event.update(overrides)
    return event


def _base_features(**overrides: Any) -> dict[str, Any]:
    features: dict[str, Any] = {
        "txn_count_1m": 1,
        "txn_count_5m": 1,
        "txn_count_1h": 2,
        "amount_sum_1h": 85.0,
        "amount_mean_prior": 50.0,
        "amount_std_prior": 15.0,
        "amount_z_score": -0.5,
        "time_since_last_sec": 3600.0,
        "geo_distance_km": 2.5,
        "travel_speed_kmph": 2.5,
        "is_international": 0,
        "merchant_category_freq": 0.45,
        "hour_of_day": 14,
        "day_of_week": 3,
        "is_weekend": 0,
        "device_change_flag": 0,
        "micro_txn_count_10m": 0,
        "structuring_txn_count_24h": 0,
        "prior_count": 12,
        "is_warmup": False,
    }
    features.update(overrides)
    return features


@pytest.mark.parametrize(
    ("expected_rule", "event_overrides", "feature_overrides"),
    [
        (
            "VELOCITY_EXCEEDED",
            {"amount": 75.0},
            {"txn_count_5m": 6, "txn_count_1h": 11},
        ),
        (
            "AMOUNT_SPIKE",
            {"amount": 1800.0},
            {"is_warmup": False, "amount_z_score": 4.8},
        ),
        (
            "GEO_IMPOSSIBILITY",
            {"amount": 320.0},
            {"geo_distance_km": 7510.4, "travel_speed_kmph": 4820.5},
        ),
        (
            "CARD_TESTING",
            {"amount": 250.0},
            {"micro_txn_count_10m": 4, "txn_count_5m": 5},
        ),
        (
            "HIGH_RISK_MERCHANT",
            {"amount": 2600.0, "category": "crypto"},
            {},
        ),
        (
            "UNUSUAL_HOUR",
            {"amount": 1450.0, "event_time": "2026-11-12T03:15:00Z"},
            {"hour_of_day": 3},
        ),
        (
            "NEW_DEVICE_HIGH_AMOUNT",
            {"amount": 1500.0},
            {"device_change_flag": 1},
        ),
        (
            "RAPID_SUCCESSION",
            {"amount": 90.0},
            {"time_since_last_sec": 8.5, "txn_count_1m": 3},
        ),
        (
            "INTERNATIONAL_HIGH_AMOUNT",
            {"amount": 3200.0, "is_international": True},
            {"is_international": 1},
        ),
        (
            "STRUCTURING",
            {"amount": 9450.0},
            {"structuring_txn_count_24h": 2, "amount_sum_1h": 18900.0},
        ),
    ],
)
def test_each_of_10_rules_fires_deterministically(
    expected_rule: str,
    event_overrides: dict[str, Any],
    feature_overrides: dict[str, Any],
) -> None:
    """TC-15: Verify each of the 10 YAML fraud rules fires on its target pattern in < 2ms."""
    engine = RuleEngine()
    assert len(engine.rules) == 10
    assert engine.rule_version == "rules-v1.0"

    event = _base_event(**event_overrides)
    features = _base_features(**feature_overrides)

    result = engine.evaluate(event=event, features=features)
    assert expected_rule in result.triggered_rules
    assert result.rule_score > 0.0
    assert any(expected_rule in r for r in result.reasons)
    assert result.evaluation_time_ms < 2.0


def test_warmup_guard_prevents_false_amount_spike_for_new_customer() -> None:
    """TC-15: New customer (`is_warmup=True`) must NOT trigger `AMOUNT_SPIKE` (`Lab678_21.pdf` Sec. 4)."""
    engine = RuleEngine()
    event = _base_event(amount=1850.0, category="electronics")
    warmup_features = _base_features(
        is_warmup=True,
        prior_count=1,
        amount_z_score=12.5,  # Even if an unguarded caller passed a large z-score
    )

    result = engine.evaluate(event=event, features=warmup_features)
    assert "AMOUNT_SPIKE" not in result.triggered_rules
    assert result.triggered_rules == []
    assert result.rule_score == 0.0


def test_normal_everyday_grocery_transaction_triggers_zero_rules() -> None:
    """TC-15: Normal everyday grocery purchase triggers zero rules and yields `rule_score == 0.0`."""
    engine = RuleEngine()
    event = _base_event(amount=48.25, category="groceries")
    features = _base_features()

    result = engine.evaluate(event=event, features=features)
    assert result.triggered_rules == []
    assert result.reasons == []
    assert result.rule_score == 0.0
    assert result.has_critical_rule is False
    assert result.evaluation_time_ms < 2.0
