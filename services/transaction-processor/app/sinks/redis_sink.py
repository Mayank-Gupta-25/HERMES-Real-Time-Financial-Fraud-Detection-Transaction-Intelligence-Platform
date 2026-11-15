"""
Redis Hot State Persistence Sink (`redis_sink.py` — `STEP AARUSSH-MOD02-03` / `MOD-05-FN-03`).

Synchronizes scored transaction state, velocity ZSETs (`velocity:{customer_id}`),
latest customer fraud decisions (`customer:{customer_id}:latest_decision`), and
deduplication keys (`dedup:event:{event_id}`) to Redis with automatic in-memory fallback.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from ..config import settings
from ..jobs.aggregation_job import parse_event_time_epoch

try:
    import redis  # type: ignore[import-untyped]
    HAS_REDIS = True
except ImportError:
    redis = None  # type: ignore[assignment]
    HAS_REDIS = False


class RedisStateSink:
    """
    Persists hot state updates from scored transactions to Redis (or an in-memory
    representation when running in unit-test environments).
    """

    def __init__(self, redis_client: Optional[Any] = None) -> None:
        self.redis_client = redis_client
        self.customer_latest_decision: Dict[str, Dict[str, str]] = {}
        self.velocity_zsets: Dict[str, Dict[str, float]] = {}
        self.dedup_keys: Dict[str, int] = {}

    def persist_decision_state(self, decision_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update hot Redis state for a scored transaction:
        - `customer:{customer_id}:latest_decision` (HASH, TTL 7d)
        - `velocity:{customer_id}` (ZSET member=`event_id`, score=`event_time_epoch`, TTL 3660s)
        - `dedup:event:{event_id}` (STRING, TTL 86400s)
        """
        event_id = str(decision_record["event_id"]).strip().lower()
        customer_id = str(decision_record["customer_id"]).strip().lower()
        event_time = str(decision_record["event_time"])
        event_epoch = parse_event_time_epoch(event_time)

        latest_summary = {
            "event_id": event_id,
            "event_time": event_time,
            "amount": str(decision_record.get("amount", 0.0)),
            "fraud_score": str(decision_record.get("fraud_score", 0.0)),
            "risk_band": str(decision_record.get("risk_band", "low")),
            "decision": str(decision_record.get("decision", "approve")),
            "model_version": str(decision_record.get("model_version", "xgboost-v1.0.0")),
            "rule_version": str(decision_record.get("rule_version", "rules-v1.0.0")),
        }

        self.customer_latest_decision[customer_id] = latest_summary
        zset = self.velocity_zsets.setdefault(customer_id, {})
        zset[event_id] = event_epoch
        self.dedup_keys[f"dedup:event:{event_id}"] = settings.dedup_ttl_sec

        if self.redis_client is not None:
            try:
                latest_key = f"customer:{customer_id}:latest_decision"
                self.redis_client.hset(latest_key, mapping=latest_summary)
                self.redis_client.expire(latest_key, 604800)

                vel_key = f"velocity:{customer_id}"
                if hasattr(self.redis_client, "zadd"):
                    self.redis_client.zadd(vel_key, {event_id: event_epoch})
                    self.redis_client.expire(vel_key, 3660)

                dedup_key = f"dedup:event:{event_id}"
                self.redis_client.set(dedup_key, "1", ex=settings.dedup_ttl_sec)
            except Exception:
                pass

        return latest_summary

    def persist_batch(self, decision_records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.persist_decision_state(rec) for rec in decision_records]

    def get_velocity_count(
        self, customer_id: str, start_epoch: float, end_epoch: float
    ) -> int:
        zset = self.velocity_zsets.get(customer_id.strip().lower(), {})
        return sum(1 for score in zset.values() if start_epoch <= score <= end_epoch)
