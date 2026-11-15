from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Awaitable, Callable

from ..config import settings
from ..db.postgres import parse_range_bounds
from ..db.redis import redis_client


class QueryCache:
    """
    Analytics Query Cache & Polyglot Federation Envelope Wrapper (Section 10.E.3 & Lab 6-7-8 Sec. 6).
    Attaches `meta`:
      - source_store
      - freshness_sla_seconds
      - consistency_model="bounded_time_range_eventual"
      - cached
      - query_time_ms
      - start_time
      - end_time
    """

    def __init__(self) -> None:
        self._store = redis_client

    @staticmethod
    def build_cache_key(endpoint: str, params: dict[str, Any]) -> str:
        canonical = json.dumps({"endpoint": endpoint, "params": params}, sort_keys=True, default=str)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
        return f"cache:analytics:{digest}"

    def invalidate_all(self) -> None:
        self._store.clear_cache()

    async def execute_with_envelope(
        self,
        endpoint: str,
        params: dict[str, Any],
        fetcher: Callable[[], Awaitable[Any]],
        source_store: str,
        freshness_sla_seconds: int = 5,
        ttl_seconds: int | None = None,
        range_str: str = "24h",
        start_time: str | None = None,
        end_time: str | None = None,
        bypass_cache: bool = False,
    ) -> dict[str, Any]:
        t0 = time.perf_counter()
        cache_key = self.build_cache_key(endpoint, params)
        ttl = ttl_seconds if ttl_seconds is not None else settings.live_cache_ttl_seconds

        if start_time is None or end_time is None:
            default_start, default_end = parse_range_bounds(range_str)
            start_time = start_time or default_start
            end_time = end_time or default_end

        if not bypass_cache:
            cached_raw = self._store.get(cache_key)
            if cached_raw is not None:
                cached_data = json.loads(cached_raw)
                elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
                return {
                    "data": cached_data,
                    "meta": {
                        "source_store": source_store,
                        "freshness_sla_seconds": freshness_sla_seconds,
                        "consistency_model": "bounded_time_range_eventual",
                        "cached": True,
                        "query_time_ms": elapsed_ms,
                        "start_time": start_time,
                        "end_time": end_time,
                    },
                }

        data = await fetcher()
        if not bypass_cache:
            self._store.setex(cache_key, ttl, json.dumps(data, default=str))

        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        return {
            "data": data,
            "meta": {
                "source_store": source_store,
                "freshness_sla_seconds": freshness_sla_seconds,
                "consistency_model": "bounded_time_range_eventual",
                "cached": False,
                "query_time_ms": elapsed_ms,
                "start_time": start_time,
                "end_time": end_time,
            },
        }


query_cache = QueryCache()
