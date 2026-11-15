from __future__ import annotations

import threading
import time
from typing import Any, Iterable, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from ..config import settings

try:
    import redis as redis_lib  # type: ignore[import-not-found]
except ImportError:
    redis_lib = None  # type: ignore[assignment]


BYPE_PUBLIC_PATHS: frozenset[str] = frozenset({"/health", "/readiness", "/metrics", "/docs", "/openapi.json", "/redoc"})


class RedisRateLimiterMiddleware(BaseHTTPMiddleware):
    """Per-client per-minute rate limiter backed by Redis atomic pipeline with in-memory fallback.

    Uses `INCR ratelimit:{client_id}:{minute_bucket}` + `EXPIRE 60` and returns HTTP 429
    with `Retry-After: 60` when the request count exceeds `rate_limit_per_minute` (default 1000).
    """

    def __init__(
        self,
        app: Any,
        limit_per_minute: Optional[int] = None,
        redis_client: Optional[Any] = None,
        exempt_paths: Optional[Iterable[str]] = None,
        enabled: Optional[bool] = None,
    ) -> None:
        super().__init__(app)
        self._limit_per_minute = limit_per_minute
        self._redis_client = redis_client
        self._exempt_paths = frozenset(exempt_paths) if exempt_paths is not None else BYPE_PUBLIC_PATHS
        self._enabled = enabled
        self._memory_counts: dict[str, int] = {}
        self._lock = threading.Lock()

    @property
    def limit_per_minute(self) -> int:
        return self._limit_per_minute if self._limit_per_minute is not None else settings.rate_limit_per_minute

    @property
    def is_enabled(self) -> bool:
        return self._enabled if self._enabled is not None else settings.rate_limit_enabled

    def _get_client_id(self, request: Request) -> str:
        header_client = request.headers.get("X-Client-ID")
        if header_client and header_client.strip():
            return header_client.strip()
        if request.client and request.client.host:
            return request.client.host
        return "anonymous"

    def _increment_counter(self, key: str) -> int:
        client = self._redis_client
        if client is None and redis_lib is not None:
            try:
                client = redis_lib.Redis(
                    host=settings.redis_host,
                    port=settings.redis_port,
                    db=settings.redis_db,
                    socket_connect_timeout=0.15,
                    socket_timeout=0.15,
                    decode_responses=True,
                )
                self._redis_client = client
            except Exception:
                client = None

        if client is not None:
            try:
                pipe = client.pipeline()
                pipe.incr(key)
                pipe.expire(key, 60)
                results = pipe.execute()
                return int(results[0])
            except Exception:
                pass

        with self._lock:
            current = self._memory_counts.get(key, 0) + 1
            self._memory_counts[key] = current
            return current

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        if not self.is_enabled or request.url.path in self._exempt_paths:
            return await call_next(request)

        client_id = self._get_client_id(request)
        minute_bucket = int(time.time() // 60)
        redis_key = f"ratelimit:{client_id}:{minute_bucket}"

        request_count = self._increment_counter(redis_key)
        limit = self.limit_per_minute

        if request_count > limit:
            correlation_id = getattr(request.state, "correlation_id", None) or request.headers.get("X-Correlation-ID")
            headers = {
                "Retry-After": "60",
                "X-RateLimit-Limit": str(limit),
                "X-RateLimit-Remaining": "0",
            }
            if correlation_id:
                headers["X-Correlation-ID"] = correlation_id
            return JSONResponse(
                status_code=429,
                content={
                    "error_code": "ERR_RATE_LIMIT_EXCEEDED",
                    "detail": f"Rate limit of {limit} requests per minute exceeded for client '{client_id}'",
                    "retry_after": 60,
                    "correlation_id": correlation_id,
                },
                headers=headers,
            )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(limit)
        response.headers["X-RateLimit-Remaining"] = str(max(0, limit - request_count))
        return response
