from __future__ import annotations

from contextlib import asynccontextmanager
import time
import uuid
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .config import settings
from .middleware.auth import JWTAuthMiddleware
from .middleware.logging import CorrelationIdLoggingMiddleware
from .middleware.rate_limiter import RedisRateLimiterMiddleware
from .middleware.telemetry import OpenTelemetryTraceMiddleware, enable_windows_sandbox_iocp_wakeup
from .models.response import DLQErrorResponse

enable_windows_sandbox_iocp_wakeup()
from .routes.health import API_REQUEST_DURATION_MS, TRANSACTIONS_INGESTED_TOTAL, router as health_router
from .routes.transactions import router as transactions_router
from .services.kafka_producer import GatewayKafkaProducer, get_kafka_producer


def _classify_validation_error(exc: RequestValidationError) -> tuple[str, str]:
    """Map Pydantic RequestValidationError details to canonical Section 10.A DLQ reason codes."""
    errors = exc.errors()
    if not errors:
        return "ERR_MALFORMED_PAYLOAD", "Invalid request payload"

    first = errors[0]
    err_type = str(first.get("type", "")).lower()
    loc_parts = [str(part).lower() for part in first.get("loc", ())]
    msg = str(first.get("msg", "Validation error"))
    loc_str = ".".join(loc_parts)

    if "json_invalid" in err_type:
        return "ERR_MALFORMED_PAYLOAD", msg
    if "missing" in err_type:
        return "ERR_MISSING_REQUIRED_FIELD", f"Missing required field '{loc_str}': {msg}"
    if "amount" in loc_parts:
        return "ERR_INVALID_AMOUNT", f"amount must be > 0 ({msg})"
    if any(field in loc_parts for field in ("event_id", "transaction_id", "customer_id", "merchant_id")):
        return "ERR_INVALID_UUID", f"Invalid UUID in '{loc_str}': {msg}"
    if any(field in loc_parts for field in ("event_time", "timestamp")):
        return "ERR_INVALID_TIMESTAMP", f"Invalid timestamp in '{loc_str}': {msg}"
    if any(field in loc_parts for field in ("location", "latitude", "longitude", "country", "city")):
        return "ERR_INVALID_COORDINATES", f"Invalid location/coordinates in '{loc_str}': {msg}"
    if "schema_version" in loc_parts:
        return "ERR_UNSUPPORTED_SCHEMA_VERSION", msg
    if any(field in loc_parts for field in ("category", "merchant_category", "currency")):
        return "ERR_INVALID_CATEGORY_OR_ENUM", msg

    return "ERR_MALFORMED_PAYLOAD", f"{loc_str}: {msg}"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    if getattr(app.state, "kafka_producer", None) is None:
        app.state.kafka_producer = get_kafka_producer()
    yield
    producer: GatewayKafkaProducer | None = getattr(app.state, "kafka_producer", None)
    if producer is not None:
        producer.close()


app = FastAPI(
    title=settings.service_name,
    version="1.0.0",
    lifespan=lifespan,
)

# Register middlewares (outermost executes first: OpenTelemetryTrace -> CorrelationIdLogging -> JWTAuth -> RateLimiter)
app.add_middleware(RedisRateLimiterMiddleware)
app.add_middleware(JWTAuthMiddleware)
app.add_middleware(CorrelationIdLoggingMiddleware)
app.add_middleware(OpenTelemetryTraceMiddleware, service_name=settings.service_name)


@app.middleware("http")
async def record_prometheus_request_duration(request: Request, call_next: Any) -> Any:
    start_time = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start_time) * 1000.0
    API_REQUEST_DURATION_MS.labels(
        service=settings.service_name,
        route=request.url.path,
        method=request.method,
        status_code=str(response.status_code),
    ).observe(duration_ms)
    return response


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Route malformed or schema-invalid ingestion payloads to `hermes.dlq` with reason code (FR-02, FR-03)."""
    correlation_id = (
        getattr(request.state, "correlation_id", None)
        or request.headers.get("X-Correlation-ID")
        or str(uuid.uuid4())
    )
    error_code, detail = _classify_validation_error(exc)

    raw_payload: Any = exc.body
    if raw_payload is None:
        try:
            body_bytes = await request.body()
            raw_payload = body_bytes.decode("utf-8", errors="replace")
        except Exception:
            raw_payload = "{}"

    producer: GatewayKafkaProducer = getattr(request.app.state, "kafka_producer", None) or get_kafka_producer()
    dlq_routed = False
    try:
        producer.publish_dlq(
            raw_payload=raw_payload,
            error_code=error_code,
            error_message=detail,
            correlation_id=correlation_id,
        )
        dlq_routed = True
    except Exception:
        dlq_routed = False

    TRANSACTIONS_INGESTED_TOTAL.labels(
        source="api",
        channel="unknown",
        status="dlq",
    ).inc()

    err_response = DLQErrorResponse(
        error_code=error_code,
        detail=detail,
        dlq_routed=dlq_routed,
        correlation_id=correlation_id,
    )
    return JSONResponse(
        status_code=422,
        content=err_response.model_dump(),
        headers={"X-Correlation-ID": correlation_id},
    )


app.include_router(health_router)
app.include_router(transactions_router)


@app.get("/")
def read_root() -> dict[str, str]:
    return {"status": "ok", "service": settings.service_name}
