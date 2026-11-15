from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from ..config import settings

logger = logging.getLogger(settings.service_name)
if not logger.handlers:
    handler = logging.StreamHandler()
    logger.addHandler(handler)
logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))


class CorrelationIdLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware that reads or generates `X-Correlation-ID`, attaches it to `request.state`,
    emits structured JSON access logs, and sets `X-Correlation-ID` on the outgoing response.
    """

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        incoming_cid = request.headers.get("X-Correlation-ID")
        correlation_id = incoming_cid.strip() if incoming_cid and incoming_cid.strip() else str(uuid.uuid4())
        request.state.correlation_id = correlation_id

        start_perf = time.perf_counter()
        response = await call_next(request)
        latency_ms = round((time.perf_counter() - start_perf) * 1000.0, 3)

        response.headers["X-Correlation-ID"] = correlation_id

        log_record = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "service": settings.service_name,
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "latency_ms": latency_ms,
            "correlation_id": correlation_id,
        }
        logger.info(json.dumps(log_record))
        return response
