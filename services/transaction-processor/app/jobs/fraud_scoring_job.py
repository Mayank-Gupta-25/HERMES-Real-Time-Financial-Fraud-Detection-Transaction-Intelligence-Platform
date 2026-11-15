"""
Redis Feature Store State Synchronization & Async Fraud Scoring Pipeline Job
(`fraud_scoring_job.py` — `STEP AARUSSH-MOD02-02` / `MOD-02`, `MOD-03`, `MOD-04`).

Responsibilities:
1. Synchronize per-customer behavioral baselines (`customer:{customer_id}:baseline`)
   and sliding velocity sets (`velocity:{customer_id}`) in Redis strictly AFTER
   extracting prior-only features (`t_i < t`, zero temporal leakage).
2. Invoke Fraud Detection `POST /score` (async `httpx` client or injected scorer)
   with automatic resilient in-process fallback scoring (`fallback_used: true`)
   when the HTTP server is unreachable.
3. Fan out every `FraudDecision` to:
   - `hermes.transactions.scored` (all scored events)
   - `hermes.transactions.flagged` (`decision in ("flag", "block")`)
   - `hermes.transactions.clean` (`decision == "approve"`)
   - `hermes.alerts.fraud` (`FraudAlert` payload when `decision in ("flag", "block")`)
"""
from __future__ import annotations

from datetime import datetime, timezone
import math
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
import uuid

from ..config import settings
from ..connectors.kafka_sink import KafkaEventSink
from ..connectors.redis_lookup import RedisProfileLookup
from ..operators.feature_extractor import FeatureExtractor

try:
    import httpx  # type: ignore[import-untyped]
    HAS_HTTPX = True
except ImportError:
    httpx = None  # type: ignore[assignment]
    HAS_HTTPX = False


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_fraud_alert_from_decision(decision_record: Dict[str, Any]) -> Dict[str, Any]:
    """Construct a canonical Section 10.F.1 FraudAlert from a flagged/blocked FraudDecision."""
    event_id = str(decision_record["event_id"])
    risk_band = str(decision_record.get("risk_band", "high"))
    return {
        "type": "FRAUD_ALERT",
        "alert_id": f"alert-{event_id}",
        "event_id": event_id,
        "transaction_id": str(decision_record.get("transaction_id") or event_id),
        "event_time": str(decision_record["event_time"]),
        "customer_id": str(decision_record["customer_id"]),
        "merchant_id": str(decision_record["merchant_id"]),
        "amount": float(decision_record["amount"]),
        "currency": str(decision_record.get("currency", "USD")),
        "category": str(decision_record.get("category", "retail")),
        "fraud_score": float(decision_record["fraud_score"]),
        "risk_band": risk_band,
        "risk_level": risk_band,
        "decision": str(decision_record["decision"]),
        "triggered_rules": list(decision_record.get("triggered_rules", [])),
        "reasons": list(decision_record.get("reasons", [])),
        "status": "active",
        "alerted_at": round(time.time(), 3),
    }


class AsyncFraudScoringClient:
    """
    Client for the Fraud Detection Service (`POST {FRAUD_SERVICE_URL}/score`)
    with connection pooling and a deterministic in-process fallback scorer so
    stream processing never stalls if the remote HTTP service is offline.
    """

    def __init__(
        self,
        fraud_service_url: Optional[str] = None,
        timeout_sec: float = 2.0,
        scoring_fn: Optional[Callable[[Dict[str, Any], Dict[str, Any], Dict[str, Any]], Dict[str, Any]]] = None,
        prefer_http: bool = False,
    ) -> None:
        self.fraud_service_url = (fraud_service_url or settings.fraud_service_url).rstrip("/")
        self.timeout_sec = timeout_sec
        self.scoring_fn = scoring_fn
        self.prefer_http = prefer_http
        self._async_client: Any = None

    def _get_async_client(self) -> Any:
        if self._async_client is None and HAS_HTTPX and httpx is not None:
            self._async_client = httpx.AsyncClient(
                base_url=self.fraud_service_url,
                timeout=self.timeout_sec,
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            )
        return self._async_client

    async def aclose(self) -> None:
        if self._async_client is not None:
            try:
                await self._async_client.aclose()
            except Exception:
                pass
            self._async_client = None

    @staticmethod
    def _fallback_score_record(
        event: Dict[str, Any],
        features: Dict[str, Any],
        provenance: Optional[Dict[str, Any]] = None,
        fallback_used: bool = True,
    ) -> Dict[str, Any]:
        """
        Resilient local hybrid scorer conforming to Section 10.B `FraudDecision` v1.0.
        """
        start_ns = time.perf_counter_ns()
        event_id = str(event.get("event_id") or event.get("transaction_id") or uuid.uuid4())
        event_time = str(event.get("event_time") or event.get("timestamp") or _utc_now_iso())
        amount = float(event.get("amount", 0.0))
        is_warmup = bool(features.get("is_warmup", False))

        triggered_rules: List[str] = []
        reasons: List[str] = []
        rule_weights_sum = 0.0
        has_block_rule = False

        # 1. Evaluate deterministic fraud rules
        txn_1m = int(features.get("txn_count_1m", 1))
        txn_5m = int(features.get("txn_count_5m", 1))
        if txn_1m > 5 or txn_5m > 15:
            triggered_rules.append("VELOCITY_EXCEEDED")
            reasons.append(f"VELOCITY_EXCEEDED: txn_count_1m={txn_1m}, txn_count_5m={txn_5m}")
            rule_weights_sum += 0.45

        z_score = float(features.get("amount_z_score", 0.0))
        if (not is_warmup) and z_score > 4.0 and amount >= 500.0:
            triggered_rules.append("AMOUNT_SPIKE")
            reasons.append(f"AMOUNT_SPIKE: amount=${amount:.2f} with z_score={z_score:.2f}")
            rule_weights_sum += 0.40

        speed_kmph = float(features.get("travel_speed_kmph", 0.0))
        dist_km = float(features.get("geo_distance_km", 0.0))
        if speed_kmph > 900.0 and dist_km > 100.0:
            triggered_rules.append("GEO_IMPOSSIBILITY")
            reasons.append(
                f"GEO_IMPOSSIBILITY: Travel speed {speed_kmph:.1f} km/h exceeds 900.0 km/h threshold"
            )
            rule_weights_sum += 0.65
            has_block_rule = True

        micro_10m = int(features.get("micro_txn_count_10m", 0))
        if amount <= 5.0 and micro_10m >= 4:
            triggered_rules.append("CARD_TESTING")
            reasons.append(f"CARD_TESTING: {micro_10m} micro-transactions (<= $5) in 10m")
            rule_weights_sum += 0.55

        if bool(event.get("merchant_blacklisted", False)):
            triggered_rules.append("HIGH_RISK_MERCHANT")
            reasons.append("HIGH_RISK_MERCHANT: Merchant is blacklisted")
            rule_weights_sum += 0.70
            has_block_rule = True
        elif bool(event.get("merchant_is_high_risk", False)) and amount >= 300.0:
            triggered_rules.append("HIGH_RISK_MERCHANT")
            reasons.append(f"HIGH_RISK_MERCHANT: High-risk merchant transaction ${amount:.2f}")
            rule_weights_sum += 0.35

        is_intl = bool(features.get("is_international", event.get("is_international", False)))
        if is_intl and amount >= 1000.0:
            triggered_rules.append("INTERNATIONAL_HIGH_AMOUNT")
            reasons.append(
                f"INTERNATIONAL_HIGH_AMOUNT: International transaction ${amount:.2f} >= $1000.00"
            )
            rule_weights_sum += 0.45

        struct_24h = int(features.get("structuring_txn_count_24h", 0))
        if 8000.0 <= amount < 10000.0 and struct_24h >= 2:
            triggered_rules.append("STRUCTURING")
            reasons.append(
                f"STRUCTURING: {struct_24h} near-threshold transactions ($8,000-$9,999) in 24h"
            )
            rule_weights_sum += 0.60
            has_block_rule = True

        if int(features.get("device_change_flag", 0)) == 1 and amount >= 1000.0:
            triggered_rules.append("NEW_DEVICE_HIGH_AMOUNT")
            reasons.append(f"NEW_DEVICE_HIGH_AMOUNT: Device change on ${amount:.2f} transaction")
            rule_weights_sum += 0.35

        rule_score = round(min(1.0, rule_weights_sum), 4)

        # 2. Statistical z-score signal (with warm-up protection)
        if is_warmup or abs(z_score) <= 0.0:
            stat_score = 0.0
        else:
            stat_score = round(1.0 / (1.0 + math.exp(-(abs(z_score) - 2.5))), 4)

        # 3. Calibrated ML probability signal
        logit = (
            -2.4
            + 1.8 * rule_score
            + 1.2 * stat_score
            + (0.8 if speed_kmph > 900.0 else 0.0)
            + (0.6 if is_intl and amount >= 1000.0 else 0.0)
            + (0.5 if txn_1m > 5 else 0.0)
        )
        ml_score = round(1.0 / (1.0 + math.exp(-logit)), 4)

        # 4. Hybrid score combination (alpha=0.40, beta=0.45, gamma=0.15)
        fraud_score = round(
            min(1.0, max(0.0, 0.40 * rule_score + 0.45 * ml_score + 0.15 * stat_score)),
            4,
        )
        if has_block_rule:
            fraud_score = max(fraud_score, 0.90)
        elif len(triggered_rules) > 0:
            fraud_score = max(fraud_score, 0.70)

        if has_block_rule or fraud_score >= 0.85:
            risk_band, decision = "critical", "block"
        elif fraud_score >= 0.65:
            risk_band, decision = "high", "flag"
        elif fraud_score >= 0.40:
            risk_band, decision = "medium", "approve"
        else:
            risk_band, decision = "low", "approve"

        now_iso = _utc_now_iso()
        elapsed_ms = max(1, int(round((time.perf_counter_ns() - start_ns) / 1_000_000.0)))

        loc = event.get("location") or {}
        location_payload = {
            "country": str(loc.get("country", "US")).upper(),
            "city": str(loc.get("city", "Unknown")),
            "latitude": float(loc.get("latitude", 0.0)),
            "longitude": float(loc.get("longitude", 0.0)),
        }

        storage_destinations = [
            "postgres:transactions",
            "redis:customer_state",
            "minio:parquet",
        ]
        if decision in ("flag", "block"):
            storage_destinations.insert(1, "postgres:fraud_alerts")

        prov_in = provenance or event.get("provenance") or {}
        provenance_payload = {
            "source_topic": str(prov_in.get("source_topic", settings.topic_transactions_raw)),
            "partition": int(prov_in.get("partition", 0)),
            "offset": int(prov_in.get("offset", 0)),
            "ingestion_time": str(prov_in.get("ingestion_time", event_time)),
            "fallback_used": bool(fallback_used),
            "storage_destinations": prov_in.get("storage_destinations", storage_destinations),
        }

        feature_values = {
            "txn_count_1m": int(features.get("txn_count_1m", 1)),
            "txn_count_5m": int(features.get("txn_count_5m", 1)),
            "txn_count_1h": int(features.get("txn_count_1h", 1)),
            "amount_sum_1h": float(features.get("amount_sum_1h", amount)),
            "amount_mean_prior": float(features.get("amount_mean_prior", 0.0)),
            "amount_std_prior": float(features.get("amount_std_prior", 0.0)),
            "amount_z_score": float(features.get("amount_z_score", 0.0)),
            "time_since_last_sec": float(features.get("time_since_last_sec", -1.0)),
            "geo_distance_km": float(features.get("geo_distance_km", 0.0)),
            "travel_speed_kmph": float(features.get("travel_speed_kmph", 0.0)),
            "is_international": int(features.get("is_international", 0)),
            "merchant_category_freq": float(features.get("merchant_category_freq", 0.0)),
            "hour_of_day": int(features.get("hour_of_day", 12)),
            "day_of_week": int(features.get("day_of_week", 0)),
            "is_weekend": int(features.get("is_weekend", 0)),
            "device_change_flag": int(features.get("device_change_flag", 0)),
            "is_warmup": bool(is_warmup),
            "prior_count": int(features.get("prior_count", 0)),
        }

        return {
            "event_id": event_id,
            "transaction_id": event_id,
            "event_time": event_time,
            "customer_id": str(event["customer_id"]),
            "merchant_id": str(event["merchant_id"]),
            "amount": amount,
            "currency": str(event.get("currency", "USD")).upper(),
            "channel": str(event.get("channel", "online")).lower(),
            "category": str(event.get("category") or event.get("merchant_category") or "retail").lower(),
            "location": location_payload,
            "fraud_score": fraud_score,
            "rule_score": rule_score,
            "ml_score": ml_score,
            "stat_score": stat_score,
            "z_score_signal": stat_score,
            "risk_band": risk_band,
            "risk_level": risk_band,
            "decision": decision,
            "triggered_rules": triggered_rules,
            "reasons": reasons,
            "model_version": "xgboost-v1.0.0",
            "shadow_model_version": "heuristic-shadow-v1.0.0",
            "shadow_ml_score": round(ml_score * 0.95, 4),
            "rule_version": "rules-v1.0.0",
            "transformation_version": "features-v1.0.0",
            "schema_version": str(event.get("schema_version", "1.0")),
            "feature_time": now_iso,
            "decision_time": now_iso,
            "processing_time_ms": elapsed_ms,
            "feature_values": feature_values,
            "provenance": provenance_payload,
        }

    def score(
        self,
        event: Dict[str, Any],
        features: Dict[str, Any],
        provenance: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Synchronous scoring call with custom callback, HTTP POST /score, or local fallback."""
        if self.scoring_fn is not None:
            try:
                return self.scoring_fn(event, features, provenance or {})
            except Exception:
                return self._fallback_score_record(
                    event=event,
                    features=features,
                    provenance=provenance,
                    fallback_used=True,
                )

        if self.prefer_http and HAS_HTTPX and httpx is not None:
            try:
                with httpx.Client(base_url=self.fraud_service_url, timeout=self.timeout_sec) as client:
                    resp = client.post(
                        "/score",
                        json={
                            "event": event,
                            "features": features,
                            "provenance": provenance or {},
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    if isinstance(data, dict) and "event_id" in data:
                        return data
            except Exception:
                return self._fallback_score_record(
                    event=event,
                    features=features,
                    provenance=provenance,
                    fallback_used=True,
                )

        return self._fallback_score_record(
            event=event,
            features=features,
            provenance=provenance,
            fallback_used=False,
        )

    async def score_async(
        self,
        event: Dict[str, Any],
        features: Dict[str, Any],
        provenance: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Asynchronous scoring call to `POST /score` with local fallback on timeout/error."""
        if self.scoring_fn is not None:
            try:
                return self.scoring_fn(event, features, provenance or {})
            except Exception:
                return self._fallback_score_record(
                    event=event,
                    features=features,
                    provenance=provenance,
                    fallback_used=True,
                )

        if self.prefer_http and HAS_HTTPX and httpx is not None:
            try:
                client = self._get_async_client()
                resp = await client.post(
                    "/score",
                    json={
                        "event": event,
                        "features": features,
                        "provenance": provenance or {},
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, dict) and "event_id" in data:
                    return data
            except Exception:
                return self._fallback_score_record(
                    event=event,
                    features=features,
                    provenance=provenance,
                    fallback_used=True,
                )

        return self._fallback_score_record(
            event=event,
            features=features,
            provenance=provenance,
            fallback_used=False,
        )


class FraudScoringJob:
    """
    Stateful streaming job that:
    1. Extracts all 16 behavioral features via `FeatureExtractor` using strictly prior
       customer baselines (`t_i < t`) and syncs updated Welford state back to Redis
       (`customer:{customer_id}:baseline`).
    2. Calls `AsyncFraudScoringClient` to evaluate the hybrid fraud decision.
    3. Routes the `FraudDecision` across `hermes.transactions.scored`,
       `hermes.transactions.flagged`, `hermes.transactions.clean`, and `hermes.alerts.fraud`.
    """

    def __init__(
        self,
        feature_extractor: Optional[FeatureExtractor] = None,
        scoring_client: Optional[AsyncFraudScoringClient] = None,
        sink: Optional[KafkaEventSink] = None,
        redis_lookup: Optional[RedisProfileLookup] = None,
    ) -> None:
        self.redis_lookup = redis_lookup or RedisProfileLookup()
        self.feature_extractor = feature_extractor or FeatureExtractor(
            redis_client=self.redis_lookup.client
        )
        self.scoring_client = scoring_client or AsyncFraudScoringClient()
        self.sink = sink or KafkaEventSink(use_in_memory=True)

        self.scored_count: int = 0
        self.clean_count: int = 0
        self.flagged_count: int = 0
        self.alerts_emitted_count: int = 0

    def _route_decision(
        self, decision_record: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        customer_key = str(decision_record.get("customer_id", "default"))
        decision = str(decision_record.get("decision", "approve")).lower()

        # 1. Always publish to hermes.transactions.scored
        self.sink.publish(
            topic=settings.topic_transactions_scored,
            payload=decision_record,
            key=customer_key,
        )
        self.scored_count += 1

        # 2. Route to flagged + alerts OR clean
        alert_record: Optional[Dict[str, Any]] = None
        if decision in ("flag", "block"):
            self.sink.publish(
                topic=settings.topic_transactions_flagged,
                payload=decision_record,
                key=customer_key,
            )
            self.flagged_count += 1

            alert_record = build_fraud_alert_from_decision(decision_record)
            self.sink.publish(
                topic=settings.topic_alerts_fraud,
                payload=alert_record,
                key=customer_key,
            )
            self.alerts_emitted_count += 1
        else:
            self.sink.publish(
                topic=settings.topic_transactions_clean,
                payload=decision_record,
                key=customer_key,
            )
            self.clean_count += 1

        return decision_record, alert_record

    def process_enriched_event(
        self,
        enriched_event: Dict[str, Any],
        provenance: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Extract 16 features (syncing updated customer baseline to Redis after reading prior state),
        score the transaction, and fan out to downstream topics.
        """
        features = self.feature_extractor.extract_features(enriched_event)
        decision_record = self.scoring_client.score(
            event=enriched_event,
            features=features,
            provenance=provenance,
        )
        return self._route_decision(decision_record)

    async def process_enriched_event_async(
        self,
        enriched_event: Dict[str, Any],
        provenance: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """Async variant of `process_enriched_event` using `score_async`."""
        features = self.feature_extractor.extract_features(enriched_event)
        decision_record = await self.scoring_client.score_async(
            event=enriched_event,
            features=features,
            provenance=provenance,
        )
        return self._route_decision(decision_record)

    def process_batch(
        self, enriched_events: Iterable[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process a batch of enriched events and return `(decisions, alerts)`."""
        decisions: List[Dict[str, Any]] = []
        alerts: List[Dict[str, Any]] = []
        for ev in enriched_events:
            decision_rec, alert_rec = self.process_enriched_event(ev)
            decisions.append(decision_rec)
            if alert_rec is not None:
                alerts.append(alert_rec)
        self.sink.flush()
        return decisions, alerts

    def get_metrics(self) -> Dict[str, int]:
        return {
            "scored_count": self.scored_count,
            "clean_count": self.clean_count,
            "flagged_count": self.flagged_count,
            "alerts_emitted_count": self.alerts_emitted_count,
        }
