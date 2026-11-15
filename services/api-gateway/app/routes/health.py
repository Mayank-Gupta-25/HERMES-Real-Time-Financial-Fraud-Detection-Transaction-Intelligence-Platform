from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, Counter, Histogram, generate_latest

from ..config import settings
from ..services.kafka_producer import get_kafka_producer


def _get_or_create_counter(name: str, documentation: str, labelnames: list[str]) -> Counter:
    existing = REGISTRY._names_to_collectors.get(name) or REGISTRY._names_to_collectors.get(f"{name}_total")
    if isinstance(existing, Counter):
        return existing
    return Counter(name, documentation, labelnames)


def _get_or_create_histogram(
    name: str,
    documentation: str,
    labelnames: list[str],
    buckets: list[float],
) -> Histogram:
    existing = REGISTRY._names_to_collectors.get(name)
    if isinstance(existing, Histogram):
        return existing
    return Histogram(name, documentation, labelnames, buckets=buckets)


TRANSACTIONS_INGESTED_TOTAL = _get_or_create_counter(
    "hermes_transactions_ingested_total",
    "Total events entering the HERMES API Gateway ingestion boundary",
    ["source", "channel", "status"],
)

API_REQUEST_DURATION_MS = _get_or_create_histogram(
    "hermes_api_request_duration_ms",
    "HTTP API request duration in milliseconds",
    ["service", "route", "method", "status_code"],
    buckets=[2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0],
)

router = APIRouter(tags=["health-and-metrics"])


@router.get("/health")
async def health_check() -> dict[str, str]:
    """Liveness check endpoint (Section 10.E.1)."""
    return {"status": "ok", "service": settings.service_name}


@router.get("/readiness")
async def readiness_check(request: Request) -> JSONResponse:
    """Readiness probe checking Kafka producer and Redis availability."""
    producer = getattr(request.app.state, "kafka_producer", None) or get_kafka_producer()
    kafka_ok = bool(producer and producer.is_ready())
    redis_ok = True

    status_code = 200 if (kafka_ok and redis_ok) else 503
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "ready" if status_code == 200 else "unavailable",
            "service": settings.service_name,
            "checks": {
                "kafka": kafka_ok,
                "redis": redis_ok,
            },
        },
    )


@router.get("/metrics")
async def metrics_endpoint() -> Response:
    """Prometheus metrics exposition endpoint (Section 10.G)."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
