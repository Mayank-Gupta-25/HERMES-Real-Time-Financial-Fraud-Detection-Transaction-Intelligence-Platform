"""
Unit & API Tests for HERMES Analytics Service Core Endpoints & Query Cache
Module: MOD-06 (MOD-06-FN-01)
Test ID: TC-24
Owner: Kushagra
"""

from __future__ import annotations

import sys
from pathlib import Path
from fastapi.testclient import TestClient

SERVICE_DIR = Path(__file__).resolve().parents[1]
if str(SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICE_DIR))
if "app" in sys.modules and not str(getattr(sys.modules["app"], "__file__", "")).startswith(str(SERVICE_DIR)):
    for mod_name in [k for k in list(sys.modules.keys()) if k == "app" or k.startswith("app.")]:
        del sys.modules[mod_name]

from app.main import app  # noqa: E402
from app.services.query_cache import query_cache  # noqa: E402


client = TestClient(app)


def _assert_federation_envelope(payload: dict) -> None:
    assert "data" in payload
    assert "meta" in payload
    meta = payload["meta"]
    assert "source_store" in meta and isinstance(meta["source_store"], str)
    assert "freshness_sla_seconds" in meta and isinstance(meta["freshness_sla_seconds"], int)
    assert meta["consistency_model"] == "bounded_time_range_eventual"
    assert isinstance(meta["cached"], bool)
    assert isinstance(meta["query_time_ms"], (int, float))
    assert "start_time" in meta and meta["start_time"].endswith("Z")
    assert "end_time" in meta and meta["end_time"].endswith("Z")


def test_overview_endpoint_and_query_cache_hit() -> None:
    """TC-24: Verify /api/v1/analytics/overview returns KPI summary, federation meta, and <10ms cache hit."""
    query_cache.invalidate_all()

    r1 = client.get("/api/v1/analytics/overview?range=24h")
    assert r1.status_code == 200
    body1 = r1.json()
    _assert_federation_envelope(body1)
    assert body1["meta"]["cached"] is False

    data = body1["data"]
    for key in [
        "current_tps",
        "total_transactions",
        "total_volume_usd",
        "fraud_rate",
        "flagged_count",
        "blocked_count",
        "avg_amount",
        "p95_latency_ms",
        "sparklines",
    ]:
        assert key in data, f"Missing overview key: {key}"
    assert data["total_transactions"] == 500
    assert data["total_volume_usd"] > 0

    # Second identical request must hit QueryCache in <10ms
    r2 = client.get("/api/v1/analytics/overview?range=24h")
    assert r2.status_code == 200
    body2 = r2.json()
    _assert_federation_envelope(body2)
    assert body2["meta"]["cached"] is True
    assert body2["meta"]["query_time_ms"] < 10.0
    assert body2["data"]["total_transactions"] == data["total_transactions"]


def test_volume_and_fraud_rate_timeseries_endpoints() -> None:
    """TC-24: Verify /volume and /fraud-rate return TimescaleDB time-bucketed arrays."""
    query_cache.invalidate_all()

    rv = client.get("/api/v1/analytics/volume?interval=5m&range=24h")
    assert rv.status_code == 200
    bv = rv.json()
    _assert_federation_envelope(bv)
    assert bv["meta"]["source_store"] == "timescaledb"
    assert isinstance(bv["data"], list) and len(bv["data"]) == 288
    first_bucket = bv["data"][0]
    for k in ["bucket_time", "tx_count", "tps", "total_amount", "avg_amount", "fraud_count", "fraud_rate"]:
        assert k in first_bucket

    rf = client.get("/api/v1/analytics/fraud-rate?interval=5m&range=24h")
    assert rf.status_code == 200
    bf = rf.json()
    _assert_federation_envelope(bf)
    assert "series" in bf["data"]
    assert "risk_band_breakdown" in bf["data"]
    assert len(bf["data"]["risk_band_breakdown"]) == 4


def test_categories_geo_and_anomalies_endpoints() -> None:
    """TC-24: Verify /categories, /geo, and /anomalies return structured analytical datasets."""
    query_cache.invalidate_all()

    rc = client.get("/api/v1/analytics/categories?range=24h")
    assert rc.status_code == 200
    bc = rc.json()
    _assert_federation_envelope(bc)
    assert isinstance(bc["data"], list) and len(bc["data"]) > 0
    assert "pct_of_volume" in bc["data"][0]
    assert "fraud_rate" in bc["data"][0]

    rg = client.get("/api/v1/analytics/geo?range=24h")
    assert rg.status_code == 200
    bg = rg.json()
    _assert_federation_envelope(bg)
    assert isinstance(bg["data"], list) and len(bg["data"]) > 0
    for k in ["country", "city", "latitude", "longitude", "tx_count", "total_amount", "fraud_count", "fraud_rate"]:
        assert k in bg["data"][0]

    ra = client.get("/api/v1/analytics/anomalies?limit=50&range=24h")
    assert ra.status_code == 200
    ba = ra.json()
    _assert_federation_envelope(ba)
    assert isinstance(ba["data"], list) and len(ba["data"]) > 0
    assert len(ba["data"]) <= 50
    for item in ba["data"]:
        assert float(item["fraud_score"]) >= 0.40
        assert "triggered_rules" in item
