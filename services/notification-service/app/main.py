from __future__ import annotations

from contextlib import asynccontextmanager
import time
from typing import Any, AsyncIterator
import uuid

from fastapi import Body, FastAPI, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, REGISTRY, generate_latest

from .config import enable_windows_sandbox_iocp_wakeup, settings
from .consumers.alert_consumer import FraudAlertConsumer
from .handlers.email_handler import EmailAlertHandler
from .handlers.webhook_handler import WebhookAlertHandler
from .handlers.websocket_handler import ConnectionManager

enable_windows_sandbox_iocp_wakeup()


def _get_or_create_histogram(name: str, doc: str, labelnames: list[str], buckets: list[float]) -> Histogram:
    existing = REGISTRY._names_to_collectors.get(name)
    if isinstance(existing, Histogram):
        return existing
    return Histogram(name, doc, labelnames=labelnames, buckets=buckets)


def _get_or_create_gauge(name: str, doc: str, labelnames: list[str]) -> Gauge:
    existing = REGISTRY._names_to_collectors.get(name)
    if isinstance(existing, Gauge):
        return existing
    return Gauge(name, doc, labelnames=labelnames)


def _get_or_create_counter(name: str, doc: str, labelnames: list[str]) -> Counter:
    existing = REGISTRY._names_to_collectors.get(name)
    if isinstance(existing, Counter):
        return existing
    return Counter(name, doc, labelnames=labelnames)


API_REQUEST_DURATION_MS = _get_or_create_histogram(
    "hermes_api_request_duration_ms",
    "HTTP API request latency across HERMES services in milliseconds",
    labelnames=["service", "route", "method", "status_code"],
    buckets=[1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0],
)

ACTIVE_ALERTS_GAUGE = _get_or_create_gauge(
    "hermes_active_alerts_gauge",
    "Currently unacknowledged fraud alerts",
    labelnames=["risk_band"],
)

NOTIFICATIONS_SENT_TOTAL = _get_or_create_counter(
    "hermes_notifications_sent_total",
    "Total fraud alert notifications broadcast by channel",
    labelnames=["channel", "risk_band"],
)


def _init_app_state(target_app: FastAPI) -> None:
    if getattr(target_app.state, "ws_manager", None) is None:
        ws_manager = ConnectionManager()
        email_handler = EmailAlertHandler()
        webhook_handler = WebhookAlertHandler()
        alert_consumer = FraudAlertConsumer(
            ws_manager=ws_manager,
            email_handler=email_handler,
            webhook_handler=webhook_handler,
        )
        target_app.state.ws_manager = ws_manager
        target_app.state.email_handler = email_handler
        target_app.state.webhook_handler = webhook_handler
        target_app.state.alert_consumer = alert_consumer


@asynccontextmanager
async def lifespan(target_app: FastAPI) -> AsyncIterator[None]:
    _init_app_state(target_app)
    yield


app = FastAPI(
    title=settings.service_name,
    version="1.0.0",
    lifespan=lifespan,
)

_init_app_state(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def read_root() -> dict[str, str]:
    return {"status": "ok", "service": settings.service_name}


@app.get("/health")
async def health_check(request: Request) -> dict[str, Any]:
    ws_manager: ConnectionManager = request.app.state.ws_manager
    return {
        "status": "ok",
        "service": settings.service_name,
        "active_ws_clients": len(ws_manager.active_connections),
        "recent_alerts_buffered": len(ws_manager.recent_alerts),
    }


@app.get("/metrics")
async def metrics_endpoint() -> Response:
    return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/v1/alerts/recent")
async def get_recent_alerts(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    ws_manager: ConnectionManager = request.app.state.ws_manager
    items = ws_manager.get_recent_alerts(limit=limit)
    return {
        "items": items,
        "count": len(items),
        "broadcast_count": ws_manager.broadcast_count,
        "duplicate_dropped_count": ws_manager.duplicate_dropped_count,
    }


@app.post("/api/v1/notify/test")
async def publish_test_or_live_alert(
    request: Request,
    payload: dict[str, Any] = Body(default_factory=dict),
) -> dict[str, Any]:
    """
    Publish a `FRAUD_ALERT` payload through `FraudAlertConsumer` (deduplicating by
    `alert_id`/`event_id` and broadcasting over `/ws/alerts`, email, and webhook).
    """
    start_time = time.perf_counter()
    consumer: FraudAlertConsumer = request.app.state.alert_consumer
    if not payload:
        event_id = str(uuid.uuid4())
        payload = {
            "type": "FRAUD_ALERT",
            "alert_id": f"alert-{event_id}",
            "event_id": event_id,
            "transaction_id": event_id,
            "customer_id": str(uuid.uuid4()),
            "merchant_id": str(uuid.uuid4()),
            "amount": 9450.0,
            "currency": "USD",
            "category": "money_transfer",
            "fraud_score": 0.9125,
            "risk_band": "critical",
            "decision": "block",
            "triggered_rules": ["GEO_IMPOSSIBILITY", "INTERNATIONAL_HIGH_AMOUNT"],
            "status": "active",
        }

    outcome = await consumer.process_alert_message(payload)
    alert = outcome["alert"]
    risk_band = str(alert.get("risk_band", "high"))
    if outcome["dispatched"]:
        ACTIVE_ALERTS_GAUGE.labels(risk_band=risk_band).inc()
        NOTIFICATIONS_SENT_TOTAL.labels(channel="websocket", risk_band=risk_band).inc()

    duration_ms = (time.perf_counter() - start_time) * 1000.0
    API_REQUEST_DURATION_MS.labels(
        service=settings.service_name,
        route="/api/v1/notify/test",
        method="POST",
        status_code="200",
    ).observe(duration_ms)
    return outcome


@app.websocket("/ws/alerts")
async def websocket_alerts_endpoint(websocket: WebSocket) -> None:
    """
    Real-time WebSocket endpoint (`/ws/alerts`) broadcasting `FRAUD_ALERT` payloads
    in < 100ms (`Section 10.F.1`, `TC-19`).
    """
    ws_manager: ConnectionManager = websocket.app.state.ws_manager
    consumer: FraudAlertConsumer = websocket.app.state.alert_consumer
    await ws_manager.connect(websocket)
    try:
        while True:
            incoming = await websocket.receive_json()
            if isinstance(incoming, dict):
                if incoming.get("type") == "PING":
                    await websocket.send_json({"type": "PONG"})
                elif incoming.get("type") == "FRAUD_ALERT" or "event_id" in incoming or "alert_id" in incoming:
                    await consumer.process_alert_message(incoming)
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)
