"""
Unit tests for Redis Feature Store State Synchronization & Async Fraud Scoring Job
(`STEP AARUSSH-MOD02-02` / `MOD-02-FN-01`, `MOD-03-FN-02`, `MOD-04-FN-04`).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any, Dict
import uuid

import pytest

SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

from app.config import settings  # noqa: E402
from app.connectors.kafka_sink import KafkaEventSink  # noqa: E402
from app.connectors.redis_lookup import RedisProfileLookup  # noqa: E402
from app.jobs.fraud_scoring_job import (  # noqa: E402
    AsyncFraudScoringClient,
    FraudScoringJob,
)


def _make_enriched_event(
    customer_id: str,
    event_time_iso: str,
    amount: float = 80.0,
    country: str = "US",
    lat: float = 40.7128,
    lon: float = -74.0060,
    is_international: bool = False,
) -> Dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": event_time_iso,
        "customer_id": customer_id,
        "merchant_id": "a2222222-e29b-41d4-a716-446655440002",
        "amount": amount,
        "currency": "USD",
        "transaction_type": "purchase",
        "channel": "online",
        "card_type": "credit",
        "category": "electronics",
        "location": {
            "country": country,
            "city": "New York" if country == "US" else "Moscow",
            "latitude": lat,
            "longitude": lon,
        },
        "device": {
            "device_id": "dev-101",
            "ip_address": "198.51.100.77",
            "device_type": "mobile",
            "os": "iOS",
        },
        "is_international": is_international,
        "customer_home_country": "US",
        "merchant_is_high_risk": False,
        "merchant_blacklisted": False,
        "status": "pending",
        "schema_version": "1.0",
    }


def test_pipeline_routes_clean_and_flagged_decisions() -> None:
    """
    Verifies that FraudScoringJob fans out clean ('approve') and fraudulent ('block')
    decisions accurately across `hermes.transactions.scored`, `hermes.transactions.clean`,
    `hermes.transactions.flagged`, and `hermes.alerts.fraud` with 100% conservation.
    """
    sink = KafkaEventSink(use_in_memory=True)
    redis_lookup = RedisProfileLookup()

    def mock_score_fn(
        event: Dict[str, Any], features: Dict[str, Any], provenance: Dict[str, Any]
    ) -> Dict[str, Any]:
        is_fraud = float(event["amount"]) > 5000.0
        decision = "block" if is_fraud else "approve"
        risk_band = "critical" if is_fraud else "low"
        return {
            "event_id": event["event_id"],
            "transaction_id": event["event_id"],
            "event_time": event["event_time"],
            "customer_id": event["customer_id"],
            "merchant_id": event["merchant_id"],
            "amount": float(event["amount"]),
            "currency": event["currency"],
            "channel": event["channel"],
            "category": event["category"],
            "location": event["location"],
            "fraud_score": 0.92 if is_fraud else 0.08,
            "rule_score": 0.90 if is_fraud else 0.0,
            "ml_score": 0.95 if is_fraud else 0.10,
            "stat_score": 0.85 if is_fraud else 0.05,
            "z_score_signal": 0.85 if is_fraud else 0.05,
            "risk_band": risk_band,
            "risk_level": risk_band,
            "decision": decision,
            "triggered_rules": ["GEO_IMPOSSIBILITY"] if is_fraud else [],
            "reasons": ["GEO_IMPOSSIBILITY: speed > 900 km/h"] if is_fraud else [],
            "model_version": "xgboost-v1.0.0",
            "rule_version": "rules-v1.0.0",
            "transformation_version": "features-v1.0.0",
            "schema_version": "1.0",
            "feature_time": event["event_time"],
            "decision_time": event["event_time"],
            "processing_time_ms": 8,
            "feature_values": features,
            "provenance": provenance,
        }

    scoring_client = AsyncFraudScoringClient(scoring_fn=mock_score_fn)
    job = FraudScoringJob(
        scoring_client=scoring_client,
        sink=sink,
        redis_lookup=redis_lookup,
    )

    cust_id = "c1111111-e29b-41d4-a716-446655440001"
    clean_ev = _make_enriched_event(cust_id, "2026-11-12T14:30:00Z", amount=65.0)
    fraud_ev = _make_enriched_event(
        cust_id,
        "2026-11-12T14:32:00Z",
        amount=9200.0,
        country="RU",
        lat=55.7558,
        lon=37.6173,
        is_international=True,
    )

    decisions, alerts = job.process_batch([clean_ev, fraud_ev])
    metrics = job.get_metrics()

    assert len(decisions) == 2
    assert len(alerts) == 1
    assert metrics["scored_count"] == 2
    assert metrics["clean_count"] == 1
    assert metrics["flagged_count"] == 1
    assert metrics["alerts_emitted_count"] == 1
    assert metrics["scored_count"] == metrics["clean_count"] + metrics["flagged_count"]

    assert len(sink.published_by_topic[settings.topic_transactions_scored]) == 2
    assert len(sink.published_by_topic[settings.topic_transactions_clean]) == 1
    assert len(sink.published_by_topic[settings.topic_transactions_flagged]) == 1
    assert len(sink.published_by_topic[settings.topic_alerts_fraud]) == 1

    alert_payload = sink.published_by_topic[settings.topic_alerts_fraud][0]["payload"]
    assert alert_payload["type"] == "FRAUD_ALERT"
    assert alert_payload["alert_id"] == f"alert-{fraud_ev['event_id']}"
    assert alert_payload["risk_band"] == "critical"
    assert alert_payload["decision"] == "block"


def test_redis_baseline_synced_without_temporal_leakage() -> None:
    """
    TC-14 (`MOD-03-FN-02`): Verifies that `customer:{customer_id}:baseline` in Redis
    is read strictly from prior events (`t_i < t`) before emitting the feature vector,
    applies the 5-event warm-up guard (`prior_count < 5` -> `is_warmup=True`, `amount_z_score=0.0`),
    and updates Redis state after each event.
    """
    redis_lookup = RedisProfileLookup()
    sink = KafkaEventSink(use_in_memory=True)
    job = FraudScoringJob(sink=sink, redis_lookup=redis_lookup)

    cust_id = "c9999999-e29b-41d4-a716-446655440099"
    base_dt = datetime(2026, 11, 12, 10, 0, 0, tzinfo=timezone.utc)
    prior_amounts = [100.0, 110.0, 90.0, 105.0, 95.0]  # mean = 100.0

    for idx, amt in enumerate(prior_amounts):
        ts = (base_dt + timedelta(minutes=idx * 5)).isoformat().replace("+00:00", "Z")
        ev = _make_enriched_event(cust_id, ts, amount=amt)
        decision_rec, _ = job.process_enriched_event(ev)
        fv = decision_rec["feature_values"]
        # Prior count must reflect strictly prior events (0..4)
        assert fv["prior_count"] == idx
        assert fv["is_warmup"] is True
        assert fv["amount_z_score"] == 0.0

        # Confirm Redis hash was synced AFTER feature emission with updated count (idx + 1)
        redis_hash = redis_lookup.client.hgetall(f"customer:{cust_id}:baseline")
        assert int(redis_hash["prior_count"]) == idx + 1

    # 6th event has prior_count == 5 -> warm-up ends, prior mean is 100.0 (does NOT include current 600.0!)
    sixth_ts = (base_dt + timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
    sixth_ev = _make_enriched_event(cust_id, sixth_ts, amount=600.0)
    sixth_decision, _ = job.process_enriched_event(sixth_ev)
    sixth_fv = sixth_decision["feature_values"]

    assert sixth_fv["prior_count"] == 5
    assert sixth_fv["is_warmup"] is False
    assert sixth_fv["amount_mean_prior"] == pytest.approx(100.0, abs=1e-3)
    assert sixth_fv["amount_z_score"] > 10.0


def test_fallback_scorer_when_http_service_unavailable() -> None:
    """
    Verifies that when the remote HTTP scoring call fails or times out,
    `AsyncFraudScoringClient` uses its resilient local fallback scorer, sets
    `provenance.fallback_used = True`, and accurately blocks a geo-impossible event.
    """
    def failing_http_call(
        _event: Dict[str, Any], _features: Dict[str, Any], _prov: Dict[str, Any]
    ) -> Dict[str, Any]:
        raise TimeoutError("Simulated /score HTTP timeout")

    scoring_client = AsyncFraudScoringClient(scoring_fn=failing_http_call)
    sink = KafkaEventSink(use_in_memory=True)
    job = FraudScoringJob(scoring_client=scoring_client, sink=sink)

    cust_id = "c7777777-e29b-41d4-a716-446655440077"
    # Event 1 in New York
    ev1 = _make_enriched_event(
        cust_id,
        "2026-11-12T12:00:00Z",
        amount=45.0,
        country="US",
        lat=40.7128,
        lon=-74.0060,
    )
    # Event 2 in Moscow 3 minutes later ($5,000 international) -> GEO_IMPOSSIBILITY + INTERNATIONAL_HIGH_AMOUNT
    ev2 = _make_enriched_event(
        cust_id,
        "2026-11-12T12:03:00Z",
        amount=5000.0,
        country="RU",
        lat=55.7558,
        lon=37.6173,
        is_international=True,
    )

    dec1, alert1 = job.process_enriched_event(ev1)
    dec2, alert2 = job.process_enriched_event(ev2)

    assert dec1["provenance"]["fallback_used"] is True
    assert dec1["decision"] == "approve"
    assert alert1 is None

    assert dec2["provenance"]["fallback_used"] is True
    assert "GEO_IMPOSSIBILITY" in dec2["triggered_rules"]
    assert "INTERNATIONAL_HIGH_AMOUNT" in dec2["triggered_rules"]
    assert dec2["decision"] == "block"
    assert dec2["risk_band"] == "critical"
    assert alert2 is not None
    assert alert2["decision"] == "block"
