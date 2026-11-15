"""
Unit tests for the 16-Feature Streaming Extractor, Welford prior-only baseline
(zero temporal leakage), and 5-event warm-up guard (`MOD-03-FN-01`, `MOD-03-FN-02`,
`TC-13`, `TC-14`).
"""
from __future__ import annotations

import importlib.util
import math
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

PROCESSOR_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, rel_path: str):
    import sys

    if name in sys.modules:
        return sys.modules[name]
    full_path = PROCESSOR_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, full_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


feat_mod = _load_module("feature_extractor", "app/operators/feature_extractor.py")
FeatureExtractor = feat_mod.FeatureExtractor
FEATURE_COLUMNS = feat_mod.FEATURE_COLUMNS


def _build_event(
    customer_id: str,
    event_time: datetime,
    amount: float,
    category: str = "groceries",
    country: str = "US",
    lat: float = 40.7128,
    lon: float = -74.0060,
    device_type: str = "mobile",
    is_international: bool = False,
) -> dict:
    eid = str(uuid.uuid4())
    etime = event_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "event_id": eid,
        "event_time": etime,
        "customer_id": customer_id,
        "merchant_id": f"merch-{category}",
        "amount": amount,
        "currency": "USD",
        "channel": "online",
        "card_type": "credit",
        "category": category,
        "location": {
            "country": country,
            "city": "New York",
            "latitude": lat,
            "longitude": lon,
        },
        "device": {
            "device_id": "dev-1",
            "ip_address": "192.0.2.1",
            "device_type": device_type,
            "os": "iOS",
        },
        "is_international": is_international,
        "status": "pending",
        "schema_version": "1.0",
    }


def test_all_16_features_extracted_deterministically():
    """TC-13: Verify all 16 canonical feature keys exist with valid finite numeric values."""
    extractor = FeatureExtractor()
    base_time = datetime(2026, 11, 14, 15, 30, 0, tzinfo=timezone.utc)  # Saturday (weekend)

    ev1 = _build_event(
        customer_id="cust-16feat",
        event_time=base_time,
        amount=120.0,
        category="electronics",
        device_type="mobile",
    )
    f1 = extractor.extract_features(ev1)

    assert len(FEATURE_COLUMNS) == 16
    for col in FEATURE_COLUMNS:
        assert col in f1, f"Missing canonical feature {col}"
        val = f1[col]
        assert isinstance(val, (int, float))
        assert math.isfinite(float(val))

    assert f1["time_since_last_sec"] == -1.0
    assert f1["geo_distance_km"] == 0.0
    assert f1["travel_speed_kmph"] == 0.0
    assert f1["hour_of_day"] == 15
    assert f1["day_of_week"] == 5  # Saturday
    assert f1["is_weekend"] == 1
    assert f1["device_change_flag"] == 0

    # Second transaction 2 minutes later on a desktop device in London
    ev2 = _build_event(
        customer_id="cust-16feat",
        event_time=base_time + timedelta(minutes=2),
        amount=240.0,
        category="electronics",
        country="GB",
        lat=51.5074,
        lon=-0.1278,
        device_type="desktop",
        is_international=True,
    )
    f2 = extractor.extract_features(ev2)
    assert f2["txn_count_5m"] == 2
    assert f2["time_since_last_sec"] == pytest.approx(120.0)
    assert f2["geo_distance_km"] > 5500.0
    assert f2["travel_speed_kmph"] > 100_000.0
    assert f2["is_international"] == 1
    assert f2["merchant_category_freq"] == pytest.approx(1.0)
    assert f2["device_change_flag"] == 1

    vec = extractor.to_feature_vector(f2)
    assert len(vec) == 16


def test_no_temporal_leakage_in_trailing_baseline():
    """
    TC-14: Feed 5 transactions of $100.00 followed by a 6th transaction of $5,000.00.
    Verify that on the 6th transaction:
      - `amount_mean_prior == 100.0` (strictly excludes the $5,000.00 current event!)
      - `is_warmup == False`
      - `amount_z_score > 100.0`
    And only on the 7th transaction does `amount_mean_prior` reflect the $5,000.00 event.
    """
    extractor = FeatureExtractor()
    cust_id = "cust-leakage-guard"
    t0 = datetime(2026, 11, 12, 9, 0, 0, tzinfo=timezone.utc)

    for i in range(5):
        ev = _build_event(cust_id, t0 + timedelta(minutes=i * 10), amount=100.0)
        feats = extractor.extract_features(ev)
        assert feats["is_warmup"] is True
        assert feats["amount_z_score"] == 0.0

    # 6th transaction: $5,000.00
    ev6 = _build_event(cust_id, t0 + timedelta(minutes=50), amount=5000.0)
    f6 = extractor.extract_features(ev6)
    assert f6["prior_count"] == 5
    assert f6["is_warmup"] is False
    assert f6["amount_mean_prior"] == pytest.approx(100.0)
    assert f6["amount_std_prior"] == pytest.approx(0.0)
    # With std floor at 1.0, z = (5000 - 100) / 1.0 = 4900.0 > 100.0
    assert f6["amount_z_score"] == pytest.approx(4900.0)
    assert f6["amount_z_score"] > 100.0

    # 7th transaction: now prior_count == 6 and mean includes the $5,000.00 6th event
    ev7 = _build_event(cust_id, t0 + timedelta(minutes=60), amount=100.0)
    f7 = extractor.extract_features(ev7)
    assert f7["prior_count"] == 6
    expected_mean_after_6 = (5 * 100.0 + 5000.0) / 6.0
    assert f7["amount_mean_prior"] == pytest.approx(expected_mean_after_6, rel=1e-3)
    assert f7["amount_std_prior"] > 1500.0


def test_warmup_period_suppresses_unstable_z_score():
    """TC-14: Verify events 1 through 5 have `is_warmup == True` and `amount_z_score == 0.0`."""
    extractor = FeatureExtractor()
    cust_id = "cust-warmup-check"
    t0 = datetime(2026, 11, 12, 10, 0, 0, tzinfo=timezone.utc)

    amounts = [10.0, 2500.0, 15.0, 8000.0, 50.0]
    for idx, amt in enumerate(amounts, start=1):
        ev = _build_event(cust_id, t0 + timedelta(minutes=idx), amount=amt)
        feats = extractor.extract_features(ev)
        assert feats["prior_count"] == idx - 1
        assert feats["is_warmup"] is True
        assert feats["amount_z_score"] == 0.0
