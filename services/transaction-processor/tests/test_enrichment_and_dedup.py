"""
Unit tests for PyFlink Kafka Connectors, Event-Time Watermarks, `event_id` Deduplication,
and Redis Enrichment Job (`MOD-02-FN-01` / `TC-10` and `MOD-02-FN-03` / `TC-12`).
"""
from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import pytest

SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

from app.config import settings  # noqa: E402
from app.connectors.kafka_sink import KafkaEventSink  # noqa: E402
from app.connectors.kafka_source import KafkaEventSource  # noqa: E402
from app.connectors.redis_lookup import RedisProfileLookup  # noqa: E402
from app.jobs.enrichment_job import (  # noqa: E402
    EnrichmentStreamWorker,
    build_pyflink_enrichment_topology,
)


def _build_event(
    event_id: str | None = None,
    customer_id: str | None = None,
    merchant_id: str | None = None,
    event_time_iso: str = "2026-11-12T14:30:00Z",
    amount: float = 120.0,
    country: str = "GB",
) -> Dict[str, Any]:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_time": event_time_iso,
        "customer_id": customer_id or "c1111111-e29b-41d4-a716-446655440001",
        "merchant_id": merchant_id or "a2222222-e29b-41d4-a716-446655440002",
        "amount": amount,
        "currency": "USD",
        "transaction_type": "purchase",
        "channel": "online",
        "card_type": "credit",
        "category": "electronics",
        "location": {
            "country": country,
            "city": "London",
            "latitude": 51.5074,
            "longitude": -0.1278,
        },
        "device": {
            "device_id": "dev-001",
            "ip_address": "198.51.100.50",
            "device_type": "mobile",
            "os": "iOS",
        },
        "is_international": False,
        "status": "pending",
        "schema_version": "1.0",
    }


def test_enrichment_joins_customer_and_merchant_profiles() -> None:
    """
    TC-10 (`MOD-02-FN-01`): Feeds a valid TransactionEvent with seeded customer and
    merchant profile hashes in RedisProfileLookup and verifies all enriched attributes
    are joined and emitted to `hermes.transactions.enriched`.
    """
    customer_id = "c1111111-e29b-41d4-a716-446655440001"
    merchant_id = "a2222222-e29b-41d4-a716-446655440002"

    redis_lookup = RedisProfileLookup()
    redis_lookup.seed_customer_profile(
        customer_id,
        {
            "home_country": "US",
            "home_city": "Chicago",
            "home_latitude": 41.8781,
            "home_longitude": -87.6298,
            "risk_tier": "medium",
            "avg_transaction_amount": 95.50,
            "card_type": "credit",
        },
    )
    redis_lookup.seed_merchant_profile(
        merchant_id,
        {
            "name": "GlobalTech UK Ltd",
            "category": "electronics",
            "country": "GB",
            "city": "London",
            "is_high_risk": True,
            "is_blacklisted": False,
        },
    )

    source = KafkaEventSource(use_in_memory=True)
    sink = KafkaEventSink(use_in_memory=True)
    worker = EnrichmentStreamWorker(source=source, sink=sink, redis_lookup=redis_lookup)

    raw_event = _build_event(customer_id=customer_id, merchant_id=merchant_id, country="GB")
    source.push_in_memory(raw_event)

    enriched_events, dlq_events = worker.run_once()
    assert len(dlq_events) == 0
    assert len(enriched_events) == 1

    enriched = enriched_events[0]
    assert enriched["enrichment_hit"] is True
    assert enriched["customer_enrichment_hit"] is True
    assert enriched["merchant_enrichment_hit"] is True
    assert enriched["customer_risk_tier"] == "medium"
    assert enriched["customer_home_country"] == "US"
    assert enriched["customer_home_city"] == "Chicago"
    assert enriched["customer_home_lat"] == pytest.approx(41.8781)
    assert enriched["customer_home_lon"] == pytest.approx(-87.6298)
    assert enriched["customer_avg_amount"] == pytest.approx(95.50)
    assert enriched["merchant_name"] == "GlobalTech UK Ltd"
    assert enriched["merchant_is_high_risk"] is True
    assert enriched["merchant_blacklisted"] is False
    # Customer home country is US and transaction country is GB -> is_international True
    assert enriched["is_international"] is True

    published_enriched = sink.published_by_topic[settings.topic_transactions_enriched]
    assert len(published_enriched) == 1
    assert published_enriched[0]["payload"]["event_id"] == raw_event["event_id"]

    topology = build_pyflink_enrichment_topology()
    assert topology["checkpoint_mode"] == "EXACTLY_ONCE"
    assert topology["watermark_max_out_of_orderness_ms"] == 5000


def test_duplicate_event_id_counted_once() -> None:
    """
    TC-12 (`MOD-02-FN-03`): Sends the same `event_id` 3 times in a batch of 10 events
    and asserts `processed_unique_count == 8`, `duplicate_dropped_count == 2`, and
    the downstream output contains each `event_id` at most once.
    """
    redis_lookup = RedisProfileLookup()
    sink = KafkaEventSink(use_in_memory=True)
    worker = EnrichmentStreamWorker(sink=sink, redis_lookup=redis_lookup)

    duplicated_id = str(uuid.uuid4())
    batch: List[Dict[str, Any]] = []
    for idx in range(10):
        # Events at index 1, 4, and 7 share `duplicated_id` (3 occurrences -> 1 kept, 2 dropped)
        ev_id = duplicated_id if idx in (1, 4, 7) else str(uuid.uuid4())
        batch.append(_build_event(event_id=ev_id, amount=50.0 + idx))

    enriched_events, dlq_events = worker.process_batch(batch)
    metrics = worker.get_metrics()

    assert len(dlq_events) == 0
    assert len(enriched_events) == 8
    assert metrics["produced_count"] == 10
    assert metrics["processed_unique_count"] == 8
    assert metrics["duplicate_dropped_count"] == 2
    assert metrics["cleaning_duplicate_dropped"] == 2

    emitted_ids = [ev["event_id"] for ev in enriched_events]
    assert len(emitted_ids) == len(set(emitted_ids)) == 8
    assert emitted_ids.count(duplicated_id) == 1


def test_out_of_order_watermark_and_dlq_routing() -> None:
    """
    TC-08 & TC-12: Sends out-of-order events within and beyond the 5-second
    bounded-out-of-orderness watermark plus 1 malformed event, verifying watermark
    advancement (`max_event_time - 5000ms`), late-event detection, DLQ side-output
    routing, and strict event conservation (`produced == unique + duplicates + dlq`).
    """
    sink = KafkaEventSink(use_in_memory=True)
    worker = EnrichmentStreamWorker(sink=sink, watermark_delay_ms=5000)

    base_dt = datetime(2026, 11, 12, 14, 30, 0, tzinfo=timezone.utc)
    t0 = base_dt.isoformat().replace("+00:00", "Z")  # 0s
    t10 = (base_dt + timedelta(seconds=10)).isoformat().replace("+00:00", "Z")  # +10s (watermark -> +5s)
    t7 = (base_dt + timedelta(seconds=7)).isoformat().replace("+00:00", "Z")  # +7s (in-window, >= +5s watermark)
    t2 = (base_dt + timedelta(seconds=2)).isoformat().replace("+00:00", "Z")  # +2s (late, < +5s watermark)

    e1 = _build_event(event_time_iso=t0)
    e2 = _build_event(event_time_iso=t10)
    e3_in_window_ooo = _build_event(event_time_iso=t7)
    e4_late = _build_event(event_time_iso=t2)
    e5_invalid = _build_event(event_time_iso=t10, amount=-99.0)

    enriched_events, dlq_events = worker.process_batch(
        [e1, e2, e3_in_window_ooo, e4_late, e5_invalid]
    )

    assert len(enriched_events) == 4
    assert len(dlq_events) == 1
    assert dlq_events[0]["error_code"] == "ERR_INVALID_AMOUNT"
    assert len(sink.published_by_topic[settings.topic_dlq]) == 1

    # Check watermark progression and late flags
    assert enriched_events[0]["is_late_event"] is False
    assert enriched_events[1]["is_late_event"] is False
    # e3 (+7s) arrived after e2 (+10s -> watermark +5s), so +7s >= +5s -> not late
    assert enriched_events[2]["is_late_event"] is False
    assert enriched_events[2]["watermark_ms"] == enriched_events[1]["event_time_epoch_ms"] - 5000
    # e4 (+2s) arrived when watermark is +5s -> flagged as late event
    assert enriched_events[3]["is_late_event"] is True

    metrics = worker.get_metrics()
    assert metrics["late_event_count"] == 1
    # Conservation invariant (NFR-04)
    assert (
        metrics["produced_count"]
        == metrics["processed_unique_count"]
        + metrics["duplicate_dropped_count"]
        + metrics["dlq_count"]
    )
