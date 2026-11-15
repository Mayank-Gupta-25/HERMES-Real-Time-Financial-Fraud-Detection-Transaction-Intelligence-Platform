"""
Redis Profile & Deduplication Lookup Connector (MOD-02-FN-01 & MOD-02-FN-03).

Fetches `customer:{customer_id}:profile` and `merchant:{merchant_id}:profile`
from Redis with an in-process 60-second LRU/TTL cache, and supports atomic
`SET dedup:event:{event_id} 1 NX EX 86400` deduplication checks.
Includes a complete in-memory fallback store so unit tests run without requiring
a live `redis` package or Redis container.
"""
from __future__ import annotations

import time
from collections import OrderedDict
from typing import Any, Dict, Optional

from ..config import settings

try:
    import redis  # type: ignore[import-untyped]
    HAS_REDIS = True
except ImportError:
    redis = None  # type: ignore[assignment]
    HAS_REDIS = False


class _InMemoryRedisFallback:
    """Deterministic in-memory Redis Hash + String + TTL store for local testing."""

    def __init__(self) -> None:
        self._hashes: Dict[str, Dict[str, str]] = {}
        self._strings: Dict[str, tuple[str, Optional[float]]] = {}

    def hset(self, key: str, mapping: Dict[str, Any]) -> int:
        bucket = self._hashes.setdefault(key, {})
        for k, v in mapping.items():
            bucket[str(k)] = str(v) if not isinstance(v, bool) else ("true" if v else "false")
        return len(mapping)

    def hgetall(self, key: str) -> Dict[str, str]:
        return dict(self._hashes.get(key, {}))

    def set(self, key: str, value: Any, nx: bool = False, ex: Optional[int] = None) -> bool:
        now = time.monotonic()
        existing = self._strings.get(key)
        if existing is not None:
            _, exp = existing
            if exp is not None and now >= exp:
                del self._strings[key]
                existing = None
        if nx and existing is not None:
            return False
        expiry = (now + ex) if ex is not None else None
        self._strings[key] = (str(value), expiry)
        return True

    def get(self, key: str) -> Optional[str]:
        now = time.monotonic()
        existing = self._strings.get(key)
        if existing is None:
            return None
        val, exp = existing
        if exp is not None and now >= exp:
            del self._strings[key]
            return None
        return val

    def expire(self, key: str, seconds: int) -> bool:
        return True

    def ping(self) -> bool:
        return True


class RedisProfileLookup:
    """
    Provides low-latency customer and merchant profile lookup with a bounded
    60-second in-process LRU cache and Redis `SET NX EX` deduplication support.
    """

    def __init__(
        self,
        redis_client: Optional[Any] = None,
        cache_ttl_sec: int = 60,
        max_cache_size: int = 10000,
    ) -> None:
        self.cache_ttl_sec = cache_ttl_sec
        self.max_cache_size = max_cache_size
        self._cache: OrderedDict[str, tuple[float, Dict[str, Any]]] = OrderedDict()

        if redis_client is not None:
            self.client = redis_client
        elif HAS_REDIS and redis is not None:
            try:
                candidate = redis.Redis(
                    host=settings.redis_host,
                    port=settings.redis_port,
                    db=settings.redis_db,
                    decode_responses=True,
                    socket_connect_timeout=0.5,
                    socket_timeout=0.5,
                )
                candidate.ping()
                self.client = candidate
            except Exception:
                self.client = _InMemoryRedisFallback()
        else:
            self.client = _InMemoryRedisFallback()

    def _cache_get(self, key: str) -> Optional[Dict[str, Any]]:
        entry = self._cache.get(key)
        if entry is None:
            return None
        ts, data = entry
        if (time.monotonic() - ts) > self.cache_ttl_sec:
            del self._cache[key]
            return None
        self._cache.move_to_end(key)
        return dict(data)

    def _cache_put(self, key: str, data: Dict[str, Any]) -> None:
        self._cache[key] = (time.monotonic(), dict(data))
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_cache_size:
            self._cache.popitem(last=False)

    @staticmethod
    def _decode_hash(raw_hash: Dict[Any, Any]) -> Dict[str, Any]:
        decoded: Dict[str, Any] = {}
        for k, v in raw_hash.items():
            key_str = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else str(k)
            val_str = v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v
            decoded[key_str] = val_str
        return decoded

    def seed_customer_profile(self, customer_id: str, profile: Dict[str, Any]) -> None:
        """Seed or update `customer:{customer_id}:profile` in Redis and clear local cache."""
        key = f"customer:{customer_id}:profile"
        if hasattr(self.client, "hset"):
            self.client.hset(key, mapping=profile)
        self._cache.pop(key, None)

    def seed_merchant_profile(self, merchant_id: str, profile: Dict[str, Any]) -> None:
        """Seed or update `merchant:{merchant_id}:profile` in Redis and clear local cache."""
        key = f"merchant:{merchant_id}:profile"
        if hasattr(self.client, "hset"):
            self.client.hset(key, mapping=profile)
        self._cache.pop(key, None)

    def get_customer_profile(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """Fetch `customer:{customer_id}:profile` from LRU cache or Redis."""
        key = f"customer:{customer_id}:profile"
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        raw: Dict[Any, Any] = {}
        try:
            raw = self.client.hgetall(key) or {}
        except Exception:
            raw = {}

        if not raw:
            return None

        parsed = self._decode_hash(raw)
        profile = {
            "customer_risk_tier": str(parsed.get("risk_tier") or parsed.get("customer_risk_tier") or "low"),
            "customer_home_country": str(parsed.get("home_country") or parsed.get("customer_home_country") or "US").upper(),
            "customer_home_city": str(parsed.get("home_city") or parsed.get("customer_home_city") or "Unknown"),
            "customer_home_lat": float(parsed.get("home_latitude") or parsed.get("customer_home_lat") or 0.0),
            "customer_home_lon": float(parsed.get("home_longitude") or parsed.get("customer_home_lon") or 0.0),
            "customer_avg_amount": float(parsed.get("avg_transaction_amount") or parsed.get("customer_avg_amount") or 75.0),
            "customer_card_type": str(parsed.get("card_type") or "credit"),
            "customer_account_created_at": parsed.get("account_created_at"),
        }
        self._cache_put(key, profile)
        return dict(profile)

    def get_merchant_profile(self, merchant_id: str) -> Optional[Dict[str, Any]]:
        """Fetch `merchant:{merchant_id}:profile` from LRU cache or Redis."""
        key = f"merchant:{merchant_id}:profile"
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        raw: Dict[Any, Any] = {}
        try:
            raw = self.client.hgetall(key) or {}
        except Exception:
            raw = {}

        if not raw:
            return None

        parsed = self._decode_hash(raw)

        def _to_bool(val: Any) -> bool:
            if isinstance(val, bool):
                return val
            if val is None:
                return False
            return str(val).strip().lower() in ("1", "true", "yes", "y")

        profile = {
            "merchant_name": str(parsed.get("name") or parsed.get("merchant_name") or f"Merchant-{merchant_id[:8]}"),
            "merchant_category": str(parsed.get("category") or parsed.get("merchant_category") or "unknown").lower(),
            "merchant_country": str(parsed.get("country") or parsed.get("merchant_country") or "US").upper(),
            "merchant_city": str(parsed.get("city") or parsed.get("merchant_city") or "Unknown"),
            "merchant_is_high_risk": _to_bool(parsed.get("is_high_risk") if "is_high_risk" in parsed else parsed.get("merchant_is_high_risk")),
            "merchant_blacklisted": _to_bool(parsed.get("is_blacklisted") if "is_blacklisted" in parsed else parsed.get("merchant_blacklisted")),
        }
        self._cache_put(key, profile)
        return dict(profile)

    def mark_event_seen_nx(self, event_id: str, ttl_sec: int = 86400) -> bool:
        """
        Atomically mark `dedup:event:{event_id}` in Redis with `SET NX EX`.
        Returns True if this is the FIRST time `event_id` is seen, False if duplicate.
        """
        key = f"dedup:event:{event_id}"
        try:
            res = self.client.set(key, "1", nx=True, ex=ttl_sec)
            return bool(res)
        except Exception:
            return True
