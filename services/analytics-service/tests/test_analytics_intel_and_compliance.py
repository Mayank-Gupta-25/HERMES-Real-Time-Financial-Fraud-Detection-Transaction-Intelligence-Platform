"""
Unit & API Tests for HERMES Analytics Service Operational Explorer, Customer 360,
Fraud Performance, AML Compliance & Cold Parquet Archive Pruning
Module: MOD-06 (MOD-06-FN-02)
Test IDs: TC-25, TC-32
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


def test_paginated_transactions_and_alert_acknowledge_patch() -> None:
    """TC-24, TC-25: Verify paginated /transactions filter and PATCH /alerts/{alert_id} state transition."""
    query_cache.invalidate_all()

    # 1. Paginated transactions
    rt = client.get("/api/v1/analytics/transactions?page=1&page_size=15&risk_band=critical")
    assert rt.status_code == 200
    bt = rt.json()
    assert bt["data"]["page"] == 1
    assert bt["data"]["page_size"] == 15
    assert bt["data"]["total"] > 0
    for tx in bt["data"]["items"]:
        assert tx["risk_band"] == "critical"

    # 2. List active alerts
    ra = client.get("/api/v1/analytics/alerts?status=active&limit=20")
    assert ra.status_code == 200
    alerts = ra.json()["data"]
    assert isinstance(alerts, list) and len(alerts) > 0
    target_alert_id = alerts[0]["alert_id"]

    # 3. Acknowledge alert via PATCH
    rp = client.patch(
        f"/api/v1/analytics/alerts/{target_alert_id}",
        json={"status": "acknowledged", "analyst_notes": "Verified impossible travel anomaly"},
    )
    assert rp.status_code == 200
    updated = rp.json()["data"]
    assert updated["alert_id"] == target_alert_id
    assert updated["status"] == "acknowledged"
    assert updated["analyst_notes"] == "Verified impossible travel anomaly"


def test_customer_360_profile_combines_postgres_and_redis_baseline() -> None:
    """TC-25: Verify /customers/{customer_id}/profile returns 360 risk profile and Welford baseline."""
    query_cache.invalidate_all()

    # Get a customer_id from transactions list
    rt = client.get("/api/v1/analytics/transactions?page=1&page_size=5")
    customer_id = rt.json()["data"]["items"][0]["customer_id"]

    rc = client.get(f"/api/v1/analytics/customers/{customer_id}/profile")
    assert rc.status_code == 200
    body = rc.json()
    assert body["meta"]["source_store"] == "postgresql+redis"

    prof = body["data"]
    assert prof["customer_id"] == customer_id
    for key in [
        "risk_tier",
        "prior_count",
        "amount_mean_prior",
        "amount_std_prior",
        "txn_count_1h",
        "category_breakdown",
        "recent_transactions",
        "active_alerts",
    ]:
        assert key in prof, f"Missing customer profile key: {key}"
    assert len(prof["recent_transactions"]) > 0


def test_fraud_performance_endpoint() -> None:
    """TC-25: Verify /fraud-performance returns ROC/PR metrics, rule frequencies, and shadow comparison."""
    query_cache.invalidate_all()

    rf = client.get("/api/v1/analytics/fraud-performance?range=7d")
    assert rf.status_code == 200
    body = rf.json()
    assert body["meta"]["source_store"] == "postgresql+ml_registry"

    data = body["data"]
    assert data["auc_roc"] > 0.90
    assert data["recall"] >= 0.80
    assert isinstance(data["rule_trigger_frequencies"], list) and len(data["rule_trigger_frequencies"]) > 0
    assert isinstance(data["score_distribution"], list) and len(data["score_distribution"]) == 5
    assert "shadow_comparison" in data


def test_aml_compliance_and_archive_summary_endpoints() -> None:
    """TC-25, TC-32: Verify /compliance/aml structuring detection and /archive/summary Parquet partition pruning."""
    query_cache.invalidate_all()

    # 1. AML compliance endpoint (TC-25)
    raml = client.get("/api/v1/analytics/compliance/aml?range=7d")
    assert raml.status_code == 200
    aml_data = raml.json()["data"]
    assert len(aml_data["structuring_candidates"]) > 0
    assert aml_data["summary"]["structuring_customer_count"] > 0
    assert "rapid_cross_border_hops" in aml_data
    assert "high_value_international_transfers" in aml_data

    # 2. Cold Parquet archive summary with date partition pruning (TC-32)
    rarch = client.get("/api/v1/analytics/archive/summary?start_date=2026-11-05&end_date=2026-11-08")
    assert rarch.status_code == 200
    arch_body = rarch.json()
    assert arch_body["meta"]["source_store"] == "minio_parquet"

    arch_data = arch_body["data"]
    assert arch_data["partition_scheme"] == "event_date=YYYY-MM-DD"
    assert arch_data["compression"] == "SNAPPY"
    assert arch_data["partition_count"] == 4
    assert arch_data["total_rows"] > 0
    assert arch_data["total_volume_usd"] > 0
    for part in arch_data["partitions"]:
        assert "2026-11-05" <= part["event_date"] <= "2026-11-08"
