"""
Unit tests for static event-time window aggregations (`1m`, `5m`, `1h`) and
Haversine geo-distance / travel speed operator (`MOD-02-FN-02`, `TC-11`).
"""
from __future__ import annotations

import importlib.util
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


geo_mod = _load_module("geo_distance", "app/operators/geo_distance.py")
agg_mod = _load_module("aggregation_job", "app/jobs/aggregation_job.py")

haversine_distance_km = geo_mod.haversine_distance_km
compute_geo_velocity = geo_mod.compute_geo_velocity
EventTimeWindowAggregator = agg_mod.EventTimeWindowAggregator


def _make_event(
    customer_id: str,
    event_time: datetime,
    amount: float = 50.0,
    merchant_id: str = "m-1",
    event_id: str | None = None,
) -> dict:
    eid = event_id or str(uuid.uuid4())
    etime = event_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "event_id": eid,
        "event_time": etime,
        "customer_id": customer_id,
        "merchant_id": merchant_id,
        "amount": amount,
        "currency": "USD",
        "channel": "online",
        "category": "electronics",
        "location": {
            "country": "US",
            "city": "New York",
            "latitude": 40.7128,
            "longitude": -74.0060,
        },
        "schema_version": "1.0",
    }


def test_haversine_and_travel_speed_nyc_to_london():
    """TC-11: NYC (40.7128, -74.0060) to London (51.5074, -0.1278) in 5 minutes."""
    nyc_lat, nyc_lon = 40.7128, -74.0060
    lon_lat, lon_lon = 51.5074, -0.1278

    dist_km = haversine_distance_km(nyc_lat, nyc_lon, lon_lat, lon_lon)
    assert 5550.0 <= dist_km <= 5590.0

    t0 = 1_700_000_000.0
    t1 = t0 + 300.0  # 5 minutes later
    geo_dist, speed_kmph = compute_geo_velocity(
        curr_lat=lon_lat,
        curr_lon=lon_lon,
        curr_time_epoch=t1,
        prev_lat=nyc_lat,
        prev_lon=nyc_lon,
        prev_time_epoch=t0,
    )
    assert geo_dist == pytest.approx(dist_km, rel=1e-4)
    assert speed_kmph > 60_000.0

    # Edge cases: first transaction (prev_lat=None) and zero delta_t
    assert compute_geo_velocity(lon_lat, lon_lon, t1, None, None, None) == (0.0, 0.0)
    dist_zero_dt, speed_zero_dt = compute_geo_velocity(
        lon_lat, lon_lon, t0, nyc_lat, nyc_lon, t0
    )
    assert dist_zero_dt > 5500.0
    assert speed_zero_dt > 0.0


def test_static_1m_5m_1h_window_counts_and_sums():
    """
    TC-11: Feed synthetic events with controlled timestamps spanning 70 minutes
    (including 2 out-of-order events within the 5s watermark) and assert exact
    txn_count_1m, txn_count_5m, txn_count_1h, and amount_sum_1h values.
    """
    aggregator = EventTimeWindowAggregator(watermark_delay_sec=5.0)
    cust_id = "cust-window-test-01"
    base_time = datetime(2026, 11, 12, 12, 0, 0, tzinfo=timezone.utc)

    # Event 1 at t = 0m ($100) — falls outside the 1h window when queried at t = 70m
    res1 = aggregator.process_event(_make_event(cust_id, base_time, amount=100.0, merchant_id="m1"))
    assert res1["txn_count_1m"] == 1
    assert res1["amount_sum_1h"] == 100.0

    # Event 2 at t = 30m ($200) — inside 1h window at t = 70m (since 70m - 30m = 40m <= 60m)
    aggregator.process_event(
        _make_event(cust_id, base_time + timedelta(minutes=30), amount=200.0, merchant_id="m2")
    )

    # Event 3 at t = 66m ($50) — inside 5m and 1h windows at t = 70m (70m - 66m = 4m <= 5m)
    aggregator.process_event(
        _make_event(cust_id, base_time + timedelta(minutes=66), amount=50.0, merchant_id="m3")
    )

    # Event 4 at t = 69m 30s ($75) — inside 1m, 5m, 1h windows at t = 70m
    aggregator.process_event(
        _make_event(
            cust_id, base_time + timedelta(minutes=69, seconds=30), amount=75.0, merchant_id="m4"
        )
    )

    # Event 5 at t = 69m 50s ($25) — advances watermark to 69m 45s
    aggregator.process_event(
        _make_event(
            cust_id, base_time + timedelta(minutes=69, seconds=50), amount=25.0, merchant_id="m5"
        )
    )

    # Out-of-order Event 6 at t = 69m 47s ($30) and Event 7 at t = 69m 48s ($40)
    # Both within the 5s watermark [69m 45s, 69m 50s]!
    ooo1 = aggregator.process_event(
        _make_event(
            cust_id, base_time + timedelta(minutes=69, seconds=47), amount=30.0, merchant_id="m6"
        )
    )
    ooo2 = aggregator.process_event(
        _make_event(
            cust_id, base_time + timedelta(minutes=69, seconds=48), amount=40.0, merchant_id="m7"
        )
    )
    assert ooo1["is_out_of_order"] is True
    assert ooo1["is_late_beyond_watermark"] is False
    assert ooo2["is_out_of_order"] is True
    assert ooo2["is_late_beyond_watermark"] is False

    # Event 8 at t = 70m 00s ($80)
    final = aggregator.process_event(
        _make_event(cust_id, base_time + timedelta(minutes=70), amount=80.0, merchant_id="m8")
    )

    # At t = 70m:
    # - In 1m window [69m, 70m]: Events at 69m30s ($75), 69m47s ($30), 69m48s ($40), 69m50s ($25), 70m00s ($80) -> 5 events, sum = $250
    # - In 5m window [65m, 70m]: Above 5 + Event at 66m ($50) -> 6 events, sum = $300
    # - In 1h window [10m, 70m]: Above 6 + Event at 30m ($200) -> 7 events, sum = $500 (Event 1 at 0m is excluded!)
    assert final["txn_count_1m"] == 5
    assert final["amount_sum_1m"] == pytest.approx(250.0)
    assert final["txn_count_5m"] == 6
    assert final["amount_sum_5m"] == pytest.approx(300.0)
    assert final["txn_count_1h"] == 7
    assert final["amount_sum_1h"] == pytest.approx(500.0)
    assert final["unique_merchants_1h"] == 7

    buckets = aggregator.get_bucket_metrics()
    assert len(buckets) >= 4
    assert sum(b["tx_count"] for b in buckets) == 8
