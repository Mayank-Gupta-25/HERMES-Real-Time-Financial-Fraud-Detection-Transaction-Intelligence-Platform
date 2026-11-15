"""
Stateful Deduplication, Deterministic Cleaning & Bounded-Out-Of-Orderness Watermark Operator
(MOD-02-FN-03 & MOD-01-FN-08).

Enforces:
1. Deterministic schema validation and UTC normalization via `validate_and_normalize_event`.
2. Invalid payload side-output routing to `hermes.dlq` with canonical `error_code`.
3. Stateful `event_id` deduplication (bounded TTL cache + optional Redis `SET NX EX 86400`)
   so each `event_id` is counted at most once (`TC-12`).
4. Event-time watermark assignment (`watermark = max_event_time - 5000 ms`) and late-event tagging.
5. Strict event conservation accounting:
   `produced_count == processed_unique_count + duplicate_dropped_count + dlq_count`.
"""
from __future__ import annotations

import time
from collections import OrderedDict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..config import settings
from ..connectors.redis_lookup import RedisProfileLookup
from ..schemas.validator import parse_utc_timestamp, validate_and_normalize_event


class DeduplicationAndCleaningOperator:
    """
    Stateful streaming operator that validates, normalizes, deduplicates by `event_id`,
    and assigns 5-second bounded-out-of-orderness watermarks.
    """

    def __init__(
        self,
        watermark_delay_ms: int = settings.watermark_max_out_of_orderness_ms,
        dedup_ttl_sec: int = settings.dedup_ttl_sec,
        max_seen_cache_size: int = 200_000,
        redis_lookup: Optional[RedisProfileLookup] = None,
        source_topic: str = settings.topic_transactions_raw,
        source_service: str = "transaction-processor",
    ) -> None:
        self.watermark_delay_ms = int(watermark_delay_ms)
        self.dedup_ttl_sec = int(dedup_ttl_sec)
        self.max_seen_cache_size = int(max_seen_cache_size)
        self.redis_lookup = redis_lookup
        self.source_topic = source_topic
        self.source_service = source_service

        # Seen event_id -> monotonic expiry timestamp
        self._seen_event_ids: OrderedDict[str, float] = OrderedDict()

        # Watermark state (milliseconds since epoch)
        self.max_event_time_ms: Optional[int] = None
        self.current_watermark_ms: Optional[int] = None

        # Conservation & observability counters (NFR-04 / Section 10.G)
        self.produced_count: int = 0
        self.processed_unique_count: int = 0
        self.duplicate_dropped_count: int = 0
        self.dlq_count: int = 0
        self.late_event_count: int = 0

        self.cleaning_valid: int = 0
        self.cleaning_duplicate_dropped: int = 0
        self.cleaning_dlq_rejected: int = 0

    def _evict_expired(self, now_mono: float) -> None:
        while self._seen_event_ids:
            oldest_key, expiry = next(iter(self._seen_event_ids.items()))
            if now_mono >= expiry or len(self._seen_event_ids) > self.max_seen_cache_size:
                self._seen_event_ids.popitem(last=False)
            else:
                break

    def _is_duplicate_event_id(self, event_id: str) -> bool:
        now_mono = time.monotonic()
        self._evict_expired(now_mono)

        expiry = self._seen_event_ids.get(event_id)
        if expiry is not None and now_mono < expiry:
            return True

        if self.redis_lookup is not None:
            first_seen_in_redis = self.redis_lookup.mark_event_seen_nx(
                event_id=event_id,
                ttl_sec=self.dedup_ttl_sec,
            )
            if not first_seen_in_redis:
                self._seen_event_ids[event_id] = now_mono + self.dedup_ttl_sec
                self._seen_event_ids.move_to_end(event_id)
                return True

        self._seen_event_ids[event_id] = now_mono + self.dedup_ttl_sec
        self._seen_event_ids.move_to_end(event_id)
        return False

    def process_element(
        self, raw_payload: Dict[str, Any] | str | bytes
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Process a single incoming payload.

        Returns:
            - `(cleaned_event, None)` if valid and unique.
            - `(None, None)` if duplicate `event_id` (dropped).
            - `(None, dlq_event)` if invalid (routed to DLQ).
        """
        self.produced_count += 1

        normalized, dlq_event = validate_and_normalize_event(
            raw_payload=raw_payload,
            source_topic=self.source_topic,
            source_service=self.source_service,
        )

        if dlq_event is not None or normalized is None:
            self.dlq_count += 1
            self.cleaning_dlq_rejected += 1
            return None, dlq_event

        event_id = normalized["event_id"]
        if self._is_duplicate_event_id(event_id):
            self.duplicate_dropped_count += 1
            self.cleaning_duplicate_dropped += 1
            return None, None

        # Assign event-time timestamp & update 5s bounded-out-of-orderness watermark
        _, event_time_epoch_ms = parse_utc_timestamp(normalized["event_time"])
        assert event_time_epoch_ms is not None

        is_late = (
            self.current_watermark_ms is not None
            and event_time_epoch_ms < self.current_watermark_ms
        )
        if is_late:
            self.late_event_count += 1

        if self.max_event_time_ms is None or event_time_epoch_ms > self.max_event_time_ms:
            self.max_event_time_ms = event_time_epoch_ms
            self.current_watermark_ms = self.max_event_time_ms - self.watermark_delay_ms

        enriched_meta_event = dict(normalized)
        enriched_meta_event["event_time_epoch_ms"] = event_time_epoch_ms
        enriched_meta_event["watermark_ms"] = self.current_watermark_ms
        enriched_meta_event["is_late_event"] = is_late

        self.processed_unique_count += 1
        self.cleaning_valid += 1
        return enriched_meta_event, None

    def process_batch(
        self, payloads: Iterable[Dict[str, Any] | str | bytes]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Process a batch of payloads and return `(valid_unique_events, dlq_events)`.
        """
        valid_events: List[Dict[str, Any]] = []
        dlq_events: List[Dict[str, Any]] = []

        for item in payloads:
            cleaned, dlq = self.process_element(item)
            if cleaned is not None:
                valid_events.append(cleaned)
            elif dlq is not None:
                dlq_events.append(dlq)

        return valid_events, dlq_events

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "produced_count": self.produced_count,
            "processed_unique_count": self.processed_unique_count,
            "duplicate_dropped_count": self.duplicate_dropped_count,
            "dlq_count": self.dlq_count,
            "late_event_count": self.late_event_count,
            "cleaning_valid": self.cleaning_valid,
            "cleaning_duplicate_dropped": self.cleaning_duplicate_dropped,
            "cleaning_dlq_rejected": self.cleaning_dlq_rejected,
            "max_event_time_ms": self.max_event_time_ms,
            "current_watermark_ms": self.current_watermark_ms,
        }
