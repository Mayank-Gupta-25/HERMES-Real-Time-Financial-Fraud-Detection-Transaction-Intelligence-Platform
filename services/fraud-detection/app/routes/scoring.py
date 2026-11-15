from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Body, Request
from prometheus_client import Counter, Gauge, Histogram, REGISTRY
from pydantic import BaseModel, Field

from ..config import settings
from ..engine.ab_router import ABModelRouter
from ..engine.scorer import HybridFraudScorer


def _get_or_create_histogram(
    name: str, documentation: str, labelnames: list[str], buckets: list[float]
) -> Histogram:
    existing = REGISTRY._names_to_collectors.get(name)
    if isinstance(existing, Histogram):
        return existing
    return Histogram(name, documentation, labelnames=labelnames, buckets=buckets)


def _get_or_create_gauge(name: str, documentation: str, labelnames: list[str]) -> Gauge:
    existing = REGISTRY._names_to_collectors.get(name)
    if isinstance(existing, Gauge):
        return existing
    return Gauge(name, documentation, labelnames=labelnames)


def _get_or_create_counter(name: str, documentation: str, labelnames: list[str]) -> Counter:
    existing = REGISTRY._names_to_collectors.get(name)
    if isinstance(existing, Counter):
        return existing
    return Counter(name, documentation, labelnames=labelnames)


SCORING_LATENCY_MS = _get_or_create_histogram(
    "hermes_scoring_latency_ms",
    "Latency of /score evaluation in milliseconds",
    labelnames=["model_version"],
    buckets=[2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0],
)

FRAUD_SCORE_DISTRIBUTION = _get_or_create_histogram(
    "hermes_fraud_score_distribution",
    "Distribution of hybrid fraud scores",
    labelnames=["risk_band", "decision"],
    buckets=[0.1, 0.25, 0.4, 0.65, 0.85, 1.0],
)

ACTIVE_ALERTS_GAUGE = _get_or_create_gauge(
    "hermes_active_alerts_gauge",
    "Currently unacknowledged fraud alerts emitted by scoring service",
    labelnames=["risk_band"],
)

ALERTS_PUBLISHED_TOTAL = _get_or_create_counter(
    "hermes_fraud_alerts_published_total",
    "Total high/critical fraud alerts generated",
    labelnames=["risk_band", "decision"],
)


class ScoringRequest(BaseModel):
    """Payload accepted by `POST /score`."""

    event: dict[str, Any] = Field(default_factory=dict)
    features: dict[str, Any] | None = None
    provenance: dict[str, Any] | None = None


class BatchScoringRequest(BaseModel):
    """Payload accepted by `POST /score/batch`."""

    events: list[dict[str, Any]] = Field(default_factory=list)


router = APIRouter(tags=["scoring"])


def _get_scorer(request: Request) -> HybridFraudScorer:
    scorer = getattr(request.app.state, "scorer", None)
    if scorer is None:
        scorer = HybridFraudScorer()
        request.app.state.scorer = scorer
    return scorer


def _get_ab_router(request: Request) -> ABModelRouter:
    ab_router = getattr(request.app.state, "ab_router", None)
    if ab_router is None:
        ab_router = ABModelRouter()
        request.app.state.ab_router = ab_router
    return ab_router


def _normalize_scoring_item(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    if "event" in payload and isinstance(payload["event"], dict):
        return (
            payload["event"],
            payload.get("features") if isinstance(payload.get("features"), dict) else None,
            payload.get("provenance") if isinstance(payload.get("provenance"), dict) else None,
        )
    # Support flat transaction dict directly
    features = payload.get("features") if isinstance(payload.get("features"), dict) else None
    provenance = payload.get("provenance") if isinstance(payload.get("provenance"), dict) else None
    return payload, features, provenance


def _record_decision_telemetry(request: Request, decision_payload: dict[str, Any]) -> None:
    model_ver = str(decision_payload.get("model_version", settings.model_version))
    latency_ms = float(decision_payload.get("processing_time_ms", 0.0))
    fraud_score = float(decision_payload.get("fraud_score", 0.0))
    risk_band = str(decision_payload.get("risk_band", "low"))
    decision = str(decision_payload.get("decision", "approve"))

    SCORING_LATENCY_MS.labels(model_version=model_ver).observe(latency_ms)
    FRAUD_SCORE_DISTRIBUTION.labels(risk_band=risk_band, decision=decision).observe(fraud_score)

    if decision in ("flag", "block"):
        ACTIVE_ALERTS_GAUGE.labels(risk_band=risk_band).inc()
        ALERTS_PUBLISHED_TOTAL.labels(risk_band=risk_band, decision=decision).inc()
        recent_alerts: list[dict[str, Any]] = getattr(request.app.state, "recent_alerts", [])
        alert_record = {
            "type": "FRAUD_ALERT",
            "alert_id": f"alert-{decision_payload['event_id']}",
            "event_id": decision_payload["event_id"],
            "transaction_id": decision_payload["transaction_id"],
            "event_time": decision_payload["event_time"],
            "customer_id": decision_payload["customer_id"],
            "merchant_id": decision_payload["merchant_id"],
            "amount": decision_payload["amount"],
            "currency": decision_payload["currency"],
            "category": decision_payload["category"],
            "fraud_score": fraud_score,
            "risk_band": risk_band,
            "risk_level": risk_band,
            "decision": decision,
            "triggered_rules": decision_payload.get("triggered_rules", []),
            "status": "active",
            "alerted_at": time.time(),
        }
        recent_alerts.append(alert_record)
        if len(recent_alerts) > 200:
            del recent_alerts[:-200]
        request.app.state.recent_alerts = recent_alerts


@router.post("/score")
async def score_transaction(
    request: Request,
    payload: dict[str, Any] = Body(...),
) -> dict[str, Any]:
    """
    Evaluate a single transaction event through the 10-rule YAML engine,
    velocity/Welford baseline checker, statistical z-score scorer, and ML ensemble.
    Returns the canonical Section 10.B `FraudDecision` payload.
    """
    scorer = _get_scorer(request)
    ab_router = _get_ab_router(request)
    event_dict, features_dict, provenance_dict = _normalize_scoring_item(payload)
    decision_payload = scorer.score(
        event=event_dict,
        features=features_dict,
        provenance=provenance_dict,
    )
    ab_router.evaluate_shadow(decision_payload)
    _record_decision_telemetry(request, decision_payload)
    return decision_payload


@router.post("/score/batch")
async def score_transaction_batch(
    request: Request,
    batch_req: BatchScoringRequest,
) -> dict[str, Any]:
    """Evaluate a micro-batch of transaction events and return all `FraudDecision` payloads."""
    start_ns = time.perf_counter_ns()
    scorer = _get_scorer(request)
    ab_router = _get_ab_router(request)
    decisions: list[dict[str, Any]] = []
    for item in batch_req.events:
        event_dict, features_dict, provenance_dict = _normalize_scoring_item(item)
        decision_payload = scorer.score(
            event=event_dict,
            features=features_dict,
            provenance=provenance_dict,
        )
        ab_router.evaluate_shadow(decision_payload)
        _record_decision_telemetry(request, decision_payload)
        decisions.append(decision_payload)

    batch_ms = round((time.perf_counter_ns() - start_ns) / 1_000_000.0, 3)
    return {
        "decisions": decisions,
        "count": len(decisions),
        "processing_time_ms": batch_ms,
    }
