"""
Integration & Contract Verification Tests for HERMES React 18 Dashboard (MOD-07)
Test ID: TC-30
Owner: Kushagra
"""

from __future__ import annotations

import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
DASHBOARD_DIR = ROOT_DIR / "services" / "dashboard"


def test_dashboard_scaffold_and_package_dependencies() -> None:
    """TC-30: Verify React 18 + Vite + Tailwind CSS + Vitest scaffold files exist and validate."""
    required_scaffold_files = [
        DASHBOARD_DIR / "Dockerfile",
        DASHBOARD_DIR / "package.json",
        DASHBOARD_DIR / "vite.config.js",
        DASHBOARD_DIR / "tailwind.config.js",
        DASHBOARD_DIR / "postcss.config.js",
        DASHBOARD_DIR / "index.html",
        DASHBOARD_DIR / "src" / "main.jsx",
        DASHBOARD_DIR / "src" / "index.css",
        DASHBOARD_DIR / "src" / "App.jsx",
    ]
    for path in required_scaffold_files:
        assert path.exists(), f"Missing dashboard scaffold file: {path}"

    pkg = json.loads((DASHBOARD_DIR / "package.json").read_text(encoding="utf-8"))
    deps = pkg.get("dependencies", {})
    dev_deps = pkg.get("devDependencies", {})
    for required_dep in ["react", "react-dom", "recharts", "d3", "lucide-react", "leaflet"]:
        assert required_dep in deps, f"Missing dependency in package.json: {required_dep}"
    for required_dev in ["vite", "vitest", "tailwindcss", "@testing-library/react"]:
        assert required_dev in dev_deps, f"Missing devDependency in package.json: {required_dev}"


def test_dashboard_api_and_websocket_hooks_contracts() -> None:
    """TC-30: Verify api.js binds all 11 analytics endpoints and useWebSocket connects to /ws/alerts."""
    api_path = DASHBOARD_DIR / "src" / "services" / "api.js"
    ws_path = DASHBOARD_DIR / "src" / "hooks" / "useWebSocket.js"
    metrics_path = DASHBOARD_DIR / "src" / "hooks" / "useMetrics.js"

    assert api_path.exists()
    assert ws_path.exists()
    assert metrics_path.exists()

    api_code = api_path.read_text(encoding="utf-8")
    required_api_functions = [
        "getOverview",
        "getVolume",
        "getFraudRate",
        "getCategories",
        "getGeo",
        "getAnomalies",
        "getTransactions",
        "getAlerts",
        "acknowledgeAlert",
        "dismissAlert",
        "getCustomerProfile",
        "getFraudPerformance",
        "getAmlCompliance",
        "getArchiveSummary",
    ]
    for fn_name in required_api_functions:
        assert f"export async function {fn_name}" in api_code, f"Missing API client function: {fn_name}"

    ws_code = ws_path.read_text(encoding="utf-8")
    assert "/ws/alerts" in ws_code
    assert "FRAUD_ALERT" in ws_code
    assert "CONNECTED" in ws_code
    assert "RECONNECTING" in ws_code


def test_dashboard_all_components_pages_and_test_suites_present() -> None:
    """TC-30: Verify all 10 visual components, 4 pages, and 5 Vitest suites adhere to Section 10.F contracts."""
    expected_components = {
        "src/components/Navbar.jsx": ["wsStatus", "selectedRange", "onRangeChange"],
        "src/components/Sidebar.jsx": ["activePage", "onNavigate"],
        "src/components/MetricCard.jsx": ["title", "value", "sparklineData", "status"],
        "src/components/TransactionTable.jsx": [
          "transactions",
          "totalCount",
          "page",
          "pageSize",
          "onPageChange",
          "onFilterChange",
          "storage_destinations",
        ],
        "src/components/AlertsTable.jsx": ["alerts", "onAcknowledge", "onDismiss"],
        "src/components/charts/TransactionVolume.jsx": ["data", "loading"],
        "src/components/charts/FraudRateGauge.jsx": [
          "fraudRate",
          "flaggedCount",
          "blockedCount",
          "totalCount",
        ],
        "src/components/charts/SpendingCategories.jsx": ["categories", "loading"],
        "src/components/charts/GeoHeatmap.jsx": ["geoPoints", "loading"],
        "src/components/charts/AnomalyTimeline.jsx": ["anomalies", "loading"],
    }

    for rel_path, tokens in expected_components.items():
        full_path = DASHBOARD_DIR / rel_path
        assert full_path.exists(), f"Missing component: {full_path}"
        content = full_path.read_text(encoding="utf-8")
        for token in tokens:
            assert token in content, f"Missing contract prop/token '{token}' in {rel_path}"

    expected_pages = [
        "src/pages/OverviewPage.jsx",
        "src/pages/TransactionsPage.jsx",
        "src/pages/FraudAnalyticsPage.jsx",
        "src/pages/CustomerIntelPage.jsx",
    ]
    for page_rel in expected_pages:
        page_path = DASHBOARD_DIR / page_rel
        assert page_path.exists(), f"Missing page: {page_path}"

    expected_tests = [
        "src/__tests__/OverviewAndCharts.test.jsx",
        "src/__tests__/TransactionsAndAlerts.test.jsx",
        "src/__tests__/GeoAndAnomalyCharts.test.jsx",
        "src/__tests__/FraudAnalyticsPage.test.jsx",
        "src/__tests__/CustomerIntelAndCategories.test.jsx",
    ]
    for test_rel in expected_tests:
        test_path = DASHBOARD_DIR / test_rel
        assert test_path.exists(), f"Missing frontend test file: {test_path}"
