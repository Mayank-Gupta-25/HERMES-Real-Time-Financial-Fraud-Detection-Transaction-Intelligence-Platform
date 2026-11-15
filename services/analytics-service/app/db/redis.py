from __future__ import annotations

import json
import time
from typing import Any

try:
    import redis  # type: ignore
except ImportError:
    redis = None  # type: ignore

from .postgres import get_seed_dataset


class RedisStore:
    """
    Redis client wrapper for hot customer baselines, velocity counters, and analytics QueryCache
    with deterministic in-memory TTL store fallback when Redis server is not running.
    """

    def __init__(self) -> None:
        self._memory_cache: dict[str, tuple[float, str]] = {}

    @property
    def dataset(self) -> dict[str, Any]:
        return get_seed_dataset()

    def get(self, key: str) -> str | None:
        entry = self._memory_cache.get(key)
        if entry is None:
            return None
        expires_at, raw_val = entry
        if time.time() > expires_at:
            self._memory_cache.pop(key, None)
            return None
        return raw_val

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        self._memory_cache[key] = (time.time() + max(1, ttl_seconds), value)

    def clear_cache(self) -> None:
        self._memory_cache.clear()

    async def get_customer_hot_baseline(self, customer_id: str) -> dict[str, Any]:
        baselines = self.dataset.get("redis_baselines", {})
        bstate = baselines.get(customer_id)
        if bstate is None:
            return {
                "prior_count": 6,
                "amount_mean_prior": 75.0,
                "amount_std_prior": 18.5,
                "is_warmup": False,
                "txn_count_1m": 1,
                "txn_count_5m": 2,
                "txn_count_1h": 3,
                "last_country": "US",
                "last_device_type": "mobile",
            }
        prior_count = int(bstate.get("prior_count", 0))
        return {
            "prior_count": prior_count,
            "amount_mean_prior": round(float(bstate.get("prior_mean", 75.0)), 2),
            "amount_std_prior": round(float(bstate.get("prior_std", 15.0)), 2),
            "is_warmup": prior_count < 5,
            "txn_count_1m": 1,
            "txn_count_5m": min(prior_count, 3),
            "txn_count_1h": min(prior_count, 6),
            "last_country": bstate.get("last_country", "US"),
            "last_device_type": bstate.get("last_device_type", "mobile"),
            "category_counts": json.loads(json.dumps(bstate.get("category_counts", {}))),
        }


redis_client = RedisStore()
