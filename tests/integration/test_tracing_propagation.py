from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import uuid

from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_service_app(service_folder: str):
    service_dir = REPO_ROOT / "services" / service_folder
    if str(service_dir) in sys.path:
        sys.path.remove(str(service_dir))
    sys.path.insert(0, str(service_dir))
    for mod_name in [k for k in list(sys.modules) if k == "app" or k.startswith("app.")]:
        del sys.modules[mod_name]
    import importlib
    mod = importlib.import_module("app.main")
    return mod.app


def test_trace_and_correlation_headers_propagate_across_gateway_and_fraud_service() -> None:
    """
    TC-26: Send requests with custom `X-Correlation-ID` and W3C `traceparent`
    to `api-gateway` and `fraud-detection`, asserting identical `X-Correlation-ID`
    and valid `X-Trace-ID` / `traceparent` headers are returned.
    """
    custom_cid = f"corr-trace-{uuid.uuid4()}"
    expected_trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
    incoming_traceparent = f"00-{expected_trace_id}-00f067aa0ba902b7-01"

    # 1. Verify API Gateway header propagation
    gw_app = _load_service_app("api-gateway")
    gw_client = TestClient(gw_app)
    gw_resp = gw_client.get(
        "/health",
        headers={
            "X-Correlation-ID": custom_cid,
            "traceparent": incoming_traceparent,
        },
    )
    assert gw_resp.status_code == 200
    assert gw_resp.headers.get("X-Correlation-ID") == custom_cid
    assert gw_resp.headers.get("X-Trace-ID") == expected_trace_id
    assert gw_resp.headers.get("traceparent", "").startswith(f"00-{expected_trace_id}-")

    # 2. Verify Fraud Detection header propagation on POST /score
    fd_app = _load_service_app("fraud-detection")
    fd_client = TestClient(fd_app)
    score_resp = fd_client.post(
        "/score",
        headers={
            "X-Correlation-ID": custom_cid,
            "traceparent": gw_resp.headers["traceparent"],
        },
        json={
            "event": {
                "event_id": str(uuid.uuid4()),
                "event_time": "2026-11-12T16:00:00Z",
                "customer_id": str(uuid.uuid4()),
                "merchant_id": str(uuid.uuid4()),
                "amount": 120.0,
                "currency": "USD",
                "channel": "online",
                "category": "retail",
                "location": {
                    "country": "US",
                    "city": "Denver",
                    "latitude": 39.7392,
                    "longitude": -104.9903,
                },
            }
        },
    )
    assert score_resp.status_code == 200
    assert score_resp.headers.get("X-Correlation-ID") == custom_cid
    assert score_resp.headers.get("X-Trace-ID") == expected_trace_id
    assert score_resp.headers.get("traceparent", "").startswith(f"00-{expected_trace_id}-")

    # 3. Verify API Reference documentation file exists and documents all 4 services
    api_ref_path = REPO_ROOT / "docs" / "api-reference.md"
    assert api_ref_path.exists()
    content = api_ref_path.read_text(encoding="utf-8")
    for port in ("8000", "8001", "8002", "8003"):
        assert port in content
