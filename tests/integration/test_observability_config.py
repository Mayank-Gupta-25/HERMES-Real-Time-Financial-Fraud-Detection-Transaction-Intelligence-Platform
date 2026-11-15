from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

from fastapi.testclient import TestClient
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
PROM_DIR = REPO_ROOT / "infrastructure" / "monitoring" / "prometheus"
GRAFANA_DIR = REPO_ROOT / "infrastructure" / "monitoring" / "grafana"


def _load_service_app(service_folder: str):
    service_dir = REPO_ROOT / "services" / service_folder
    if str(service_dir) in sys.path:
        sys.path.remove(str(service_dir))
    sys.path.insert(0, str(service_dir))
    for mod_name in [k for k in list(sys.modules) if k == "app" or k.startswith("app.")]:
        del sys.modules[mod_name]
    import importlib
    mod = importlib.import_module("app.main")
    if hasattr(mod, "enable_windows_sandbox_iocp_wakeup"):
        mod.enable_windows_sandbox_iocp_wakeup()
    return mod.app


def test_prometheus_and_alert_rules_yaml_valid() -> None:
    """TC-26: Verify `prometheus.yml`, `alert-rules.yml`, and Grafana provisioning YAML files."""
    prom_cfg = yaml.safe_load((PROM_DIR / "prometheus.yml").read_text(encoding="utf-8"))
    assert prom_cfg["global"]["scrape_interval"] == "5s"
    job_names = {job["job_name"] for job in prom_cfg["scrape_configs"]}
    for expected_job in (
        "api-gateway",
        "fraud-detection",
        "analytics-service",
        "notification-service",
        "transaction-processor",
    ):
        assert expected_job in job_names

    alerts_cfg = yaml.safe_load((PROM_DIR / "alert-rules.yml").read_text(encoding="utf-8"))
    rules = alerts_cfg["groups"][0]["rules"]
    alert_names = {r["alert"] for r in rules}
    assert {
        "HighEndToEndLatency",
        "HighScoringLatency",
        "ElevatedDLQRate",
        "FraudSpikeDetected",
    }.issubset(alert_names)

    ds_cfg = yaml.safe_load(
        (GRAFANA_DIR / "provisioning" / "datasources" / "datasources.yml").read_text(encoding="utf-8")
    )
    assert ds_cfg["datasources"][0]["type"] == "prometheus"

    dash_prov = yaml.safe_load(
        (GRAFANA_DIR / "provisioning" / "dashboards" / "dashboards.yml").read_text(encoding="utf-8")
    )
    assert len(dash_prov["providers"]) >= 1


def test_grafana_dashboards_json_and_section_10g_metrics() -> None:
    """TC-26: Parse all 3 Grafana JSON dashboards and verify Section 10.G metric queries."""
    dashboards_dir = GRAFANA_DIR / "dashboards"
    expected_files = [
        "system-overview.json",
        "kafka-metrics.json",
        "fraud-metrics.json",
    ]
    all_expressions: list[str] = []

    for filename in expected_files:
        path = dashboards_dir / filename
        assert path.exists(), f"Missing dashboard file: {path}"
        data = json.loads(path.read_text(encoding="utf-8"))
        assert "title" in data and data["title"]
        assert "panels" in data and len(data["panels"]) >= 3
        for panel in data["panels"]:
            assert "title" in panel
            for target in panel.get("targets", []):
                all_expressions.append(target.get("expr", ""))

    combined_expr = " ".join(all_expressions)
    for required_metric in (
        "hermes_transactions_ingested_total",
        "hermes_cleaning_events_total",
        "hermes_scoring_latency_ms",
        "hermes_e2e_latency_ms",
        "hermes_fraud_score_distribution",
    ):
        assert required_metric in combined_expr, f"Missing metric {required_metric} in dashboards"


def test_fastapi_metrics_endpoints_expose_prometheus_format() -> None:
    """TC-26: Query `/metrics` on `api-gateway` and `fraud-detection` and verify Prometheus exposition."""
    for svc in ("api-gateway", "fraud-detection"):
        svc_app = _load_service_app(svc)
        client = TestClient(svc_app)
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "# HELP" in resp.text
        assert "hermes_api_request_duration_ms" in resp.text
