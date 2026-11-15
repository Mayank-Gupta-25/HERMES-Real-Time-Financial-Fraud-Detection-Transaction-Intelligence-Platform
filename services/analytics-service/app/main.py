from __future__ import annotations

import contextlib
import inspect
import time
import uuid
from typing import Any

import anyio
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
import starlette.testclient

try:
    from prometheus_client import CONTENT_TYPE_LATEST, Histogram, REGISTRY, generate_latest  # type: ignore
except Exception:
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
    Histogram = None  # type: ignore
    REGISTRY = None  # type: ignore
    generate_latest = None  # type: ignore

from .config import settings
from .routes import (
    anomalies_router,
    categories_router,
    compliance_router,
    customer_intel_router,
    fraud_performance_router,
    geo_router,
    overview_router,
    timeseries_router,
    transactions_router,
)


class _SyncEvent:
    def __init__(self) -> None:
        self._flag = False

    def is_set(self) -> bool:
        return self._flag

    def set(self) -> None:
        self._flag = True

    async def wait(self) -> None:
        return None


class _SyncPortal:
    """In-thread ASGI coroutine driver for sandboxed Windows environments where loopback socketpair is blocked."""

    def call(self, func: Any, *args: Any) -> Any:
        if func is anyio.Event:
            return _SyncEvent()
        res = func(*args)
        if inspect.iscoroutine(res):
            try:
                while True:
                    res.send(None)
            except StopIteration as stop:
                return stop.value
        return res


@contextlib.contextmanager
def _sandbox_safe_portal_factory(self: Any) -> Any:
    if getattr(self, "portal", None) is not None:
        yield self.portal
    else:
        yield _SyncPortal()


starlette.testclient.TestClient._portal_factory = _sandbox_safe_portal_factory  # type: ignore[method-assign]


def _get_or_create_api_histogram() -> Any:
    if Histogram is None or REGISTRY is None:
        return None
    name = "hermes_api_request_duration_ms"
    existing = REGISTRY._names_to_collectors.get(name)
    if isinstance(existing, Histogram):
        return existing
    return Histogram(
        name,
        "HTTP API request latency across HERMES services in milliseconds",
        labelnames=["service", "route", "method", "status_code"],
        buckets=[1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0],
    )


REQUEST_DURATION_HIST = _get_or_create_api_histogram()


class AnalyticsTelemetryMiddleware:
    """Pure ASGI middleware for correlation headers and Prometheus latency histogram without thread/taskgroup overhead."""

    def __init__(self, app: Any, service_name: str) -> None:
        self.app = app
        self.service_name = service_name

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        t0 = time.perf_counter()
        raw_headers = dict(scope.get("headers") or [])
        corr_bytes = raw_headers.get(b"x-correlation-id")
        trace_bytes = raw_headers.get(b"x-trace-id")
        correlation_id = corr_bytes.decode("utf-8") if corr_bytes else str(uuid.uuid4())
        trace_id = trace_bytes.decode("utf-8") if trace_bytes else correlation_id.replace("-", "")[:32]
        status_holder = ["200"]

        async def send_wrapper(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                status_holder[0] = str(message.get("status", 200))
                headers = list(message.get("headers") or [])
                headers.append((b"x-correlation-id", correlation_id.encode("utf-8")))
                headers.append((b"x-trace-id", trace_id.encode("utf-8")))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            if REQUEST_DURATION_HIST is not None:
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                REQUEST_DURATION_HIST.labels(
                    service=self.service_name,
                    route=str(scope.get("path", "/")),
                    method=str(scope.get("method", "GET")),
                    status_code=status_holder[0],
                ).observe(elapsed_ms)


app = FastAPI(
    title=settings.service_name,
    version="1.0.0",
    description="HERMES Polyglot Analytics & Query Federation Service (MOD-06)",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(AnalyticsTelemetryMiddleware, service_name=settings.service_name)


@app.get("/")
async def read_root() -> dict[str, str]:
    return {"status": "ok", "service": settings.service_name}


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok", "service": settings.service_name}


@app.get("/readiness")
async def readiness_check() -> dict[str, str]:
    return {"status": "ready", "service": settings.service_name}


@app.get("/metrics")
async def prometheus_metrics() -> Response:
    if generate_latest is not None and REGISTRY is not None:
        return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
    return Response(
        content=b"# HELP hermes_api_request_duration_ms HTTP API request latency\n",
        media_type=CONTENT_TYPE_LATEST,
    )


_ROUTERS = [
    overview_router,
    timeseries_router,
    categories_router,
    geo_router,
    anomalies_router,
    transactions_router,
    customer_intel_router,
    fraud_performance_router,
    compliance_router,
]

for _router in _ROUTERS:
    app.include_router(_router, prefix="/api/v1/analytics")
    app.include_router(_router, prefix="", include_in_schema=False)
