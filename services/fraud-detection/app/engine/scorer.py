from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path
import time
from typing import Any
import uuid

from ..config import settings
from ..models.ensemble import EnsembleModelWrapper
from .rule_engine import RuleEngine
from .velocity_checker import VelocityChecker


CATEGORY_PRIORS: dict[str, tuple[float, float]] = {
    "groceries": (65.0, 45.0),
    "dining": (45.0, 35.0),
    "retail": (90.0, 80.0),
    "electronics": (350.0, 300.0),
    "travel": (450.0, 400.0),
    "entertainment": (60.0, 50.0),
    "utilities": (120.0, 60.0),
    "healthcare": (180.0, 150.0),
    "gambling": (250.0, 250.0),
    "crypto": (500.0, 500.0),
    "money_transfer": (400.0, 450.0),
    "jewelry": (600.0, 550.0),
}


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class StatisticalScorer:
    """
    Statistical anomaly scorer mapping customer amount z-score and IQR signal
    via sigmoid transformation $\\sigma(|z| - 2.5) = \\frac{1}{1 + e^{-(|z| - 2.5)}}$ in `[0.0, 1.0]`.
    Enforces warm-up damping (`is_warmup=True`) using conservative category priors only.
    """

    @staticmethod
    def sigmoid_z(abs_z: float) -> float:
        if abs_z <= 0.0:
            return 0.0
        val = 1.0 / (1.0 + math.exp(-(abs_z - 2.5)))
        return float(min(1.0, max(0.0, round(val, 4))))

    def score(self, event: dict[str, Any], features: dict[str, Any]) -> float:
        is_warmup = bool(features.get("is_warmup", False))
        amount = float(event.get("amount", features.get("amount", 0.0)) or 0.0)
        category = str(
            event.get("category") or event.get("merchant_category") or "retail"
        ).strip().lower()

        if is_warmup:
            # During warm-up (`prior_count < 5`), never use unstable customer std dev;
            # use conservative category-level priors with a wide 4x spread.
            cat_mean, cat_std = CATEGORY_PRIORS.get(category, (150.0, 200.0))
            conservative_z = max(0.0, (amount - cat_mean) / max(cat_std * 4.0, 100.0))
            if conservative_z < 1.5:
                return 0.0
            return self.sigmoid_z(conservative_z)

        raw_z = float(features.get("amount_z_score", 0.0) or 0.0)
        abs_z = abs(raw_z)
        return self.sigmoid_z(abs_z)


class HybridFraudScorer:
    """
    Hybrid Fraud Scorer combining deterministic YAML rules ($S_{\\text{rule}}$),
    ML ensemble inference ($S_{\\text{ml}}$), and statistical z-score anomaly signal ($S_{\\text{stat}}$)
    using frozen validation weights from `scoring_weights.json`.
    Produces the exact Section 10.B `FraudDecision` payload.
    """

    def __init__(
        self,
        rule_engine: RuleEngine | None = None,
        velocity_checker: VelocityChecker | None = None,
        stat_scorer: StatisticalScorer | None = None,
        ensemble_model: EnsembleModelWrapper | None = None,
        weights_path: str | Path | None = None,
    ) -> None:
        self.rule_engine = rule_engine or RuleEngine()
        self.velocity_checker = velocity_checker or VelocityChecker()
        self.stat_scorer = stat_scorer or StatisticalScorer()
        self.ensemble_model = ensemble_model or EnsembleModelWrapper()
        self.weights_path = Path(weights_path or settings.scoring_weights_path)
        self.weights: dict[str, Any] = {}
        self.reload_weights()

    def reload_weights(self) -> dict[str, Any]:
        if not self.weights_path.exists():
            alt_path = Path(__file__).resolve().parent.parent / "rules" / "scoring_weights.json"
            if alt_path.exists():
                self.weights_path = alt_path

        if self.weights_path.exists():
            with open(self.weights_path, "r", encoding="utf-8") as fh:
                self.weights = json.load(fh)
        else:
            self.weights = {
                "weights_version": "weights-v1.0",
                "alpha_rule": 0.40,
                "beta_ml": 0.45,
                "gamma_stat": 0.15,
                "fallback_alpha_rule": 0.65,
                "fallback_gamma_stat": 0.35,
                "thresholds": {
                    "low_max": 0.40,
                    "medium_max": 0.65,
                    "high_max": 0.85,
                },
                "warmup_min_events": 5,
                "target_recall": 0.85,
            }
        return self.weights

    def classify_risk_band_and_decision(
        self,
        fraud_score: float,
        has_block_rule: bool = False,
    ) -> tuple[str, str]:
        """
        Map `fraud_score` in `[0.0, 1.0]` to `(risk_band, decision)` (`Lab678_21.pdf` Section 5):
          - `[0.00, 0.40)` -> `("low", "approve")`
          - `[0.40, 0.65)` -> `("medium", "approve")`
          - `[0.65, 0.85)` -> `("high", "flag")`
          - `[0.85, 1.00]` -> `("critical", "block")`
        """
        thresholds = self.weights.get("thresholds", {})
        low_max = float(thresholds.get("low_max", 0.40))
        medium_max = float(thresholds.get("medium_max", 0.65))
        high_max = float(thresholds.get("high_max", 0.85))

        if has_block_rule or fraud_score >= high_max:
            return "critical", "block"
        if fraud_score >= medium_max:
            return "high", "flag"
        if fraud_score >= low_max:
            return "medium", "approve"
        return "low", "approve"

    def combine_scores(
        self,
        rule_score: float,
        ml_score: float | None,
        stat_score: float,
        has_critical_rule: bool = False,
        ml_available: bool = True,
    ) -> float:
        """
        Combine $S_{\\text{rule}}$, $S_{\\text{ml}}$, and $S_{\\text{stat}}$ using frozen weights,
        with graceful fallback to `fallback_alpha_rule` + `fallback_gamma_stat` if ML is unavailable.
        """
        r_s = min(1.0, max(0.0, float(rule_score)))
        s_s = min(1.0, max(0.0, float(stat_score)))

        if ml_available and ml_score is not None:
            alpha = float(self.weights.get("alpha_rule", 0.40))
            beta = float(self.weights.get("beta_ml", 0.45))
            gamma = float(self.weights.get("gamma_stat", 0.15))
            m_s = min(1.0, max(0.0, float(ml_score)))
            combined = alpha * r_s + beta * m_s + gamma * s_s
        else:
            fb_alpha = float(self.weights.get("fallback_alpha_rule", 0.65))
            fb_gamma = float(self.weights.get("fallback_gamma_stat", 0.35))
            combined = fb_alpha * r_s + fb_gamma * s_s

        if has_critical_rule:
            combined = max(combined, 0.90)

        return float(min(1.0, max(0.0, round(combined, 4))))

    def score(
        self,
        event: dict[str, Any],
        features: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
        ml_available: bool = True,
    ) -> dict[str, Any]:
        """
        Execute end-to-end hybrid fraud evaluation and return the canonical Section 10.B
        `FraudDecision` dictionary with full provenance.
        """
        start_ns = time.perf_counter_ns()

        event_id = str(
            event.get("event_id") or event.get("transaction_id") or uuid.uuid4()
        )
        raw_event_time = event.get("event_time") or event.get("timestamp")
        if isinstance(raw_event_time, str) and raw_event_time.strip():
            event_time_str = raw_event_time.strip()
            if not event_time_str.endswith("Z") and "+" in event_time_str:
                try:
                    dt = datetime.fromisoformat(event_time_str).astimezone(timezone.utc)
                    event_time_str = dt.isoformat().replace("+00:00", "Z")
                except ValueError:
                    pass
        else:
            event_time_str = _utc_iso_now()

        # 1. Compute or enrich 16 features + velocity state without temporal leakage
        enriched_features = self.velocity_checker.compute_features(
            event=event,
            provided_features=features,
            update_state=True,
        )
        feature_time_str = str(
            enriched_features.get("feature_time") or event.get("feature_time") or _utc_iso_now()
        )

        # 2. Evaluate deterministic 10-rule YAML engine
        rule_res = self.rule_engine.evaluate(event=event, features=enriched_features)

        # 3. Evaluate statistical z-score anomaly signal (with warm-up protection)
        stat_score = self.stat_scorer.score(event=event, features=enriched_features)

        # 4. Evaluate ML ensemble (or fallback if ml_available=False)
        ml_res = self.ensemble_model.predict(
            features=enriched_features,
            force_unavailable=not ml_available,
        )
        effective_ml_available = bool(ml_res.get("available", True) and ml_available)
        ml_score = float(ml_res.get("ml_score", 0.0)) if effective_ml_available else 0.0
        shadow_ml_score = float(ml_res.get("shadow_ml_score", 0.0))

        # 5. Combine scores and determine risk_band & decision
        fraud_score = self.combine_scores(
            rule_score=rule_res.rule_score,
            ml_score=ml_score if effective_ml_available else None,
            stat_score=stat_score,
            has_critical_rule=rule_res.has_critical_rule,
            ml_available=effective_ml_available,
        )
        risk_band, decision = self.classify_risk_band_and_decision(
            fraud_score=fraud_score,
            has_block_rule=rule_res.has_block_action,
        )

        decision_time_str = _utc_iso_now()
        elapsed_ms = round((time.perf_counter_ns() - start_ns) / 1_000_000.0, 3)

        loc = event.get("location") or {}
        location_payload = {
            "country": str(loc.get("country", event.get("country", "US")) or "US").upper(),
            "city": str(loc.get("city", event.get("city", "Unknown")) or "Unknown"),
            "latitude": float(loc.get("latitude", event.get("latitude", 0.0)) or 0.0),
            "longitude": float(loc.get("longitude", event.get("longitude", 0.0)) or 0.0),
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
            "source_topic": str(prov_in.get("source_topic", "hermes.transactions.raw")),
            "partition": int(prov_in.get("partition", 0)),
            "offset": int(prov_in.get("offset", 0)),
            "ingestion_time": str(prov_in.get("ingestion_time", event_time_str)),
            "weights_version": str(self.weights.get("weights_version", "weights-v1.0")),
            "fallback_used": not effective_ml_available,
            "storage_destinations": prov_in.get("storage_destinations", storage_destinations),
        }

        feature_values_payload = {
            "txn_count_1m": int(enriched_features.get("txn_count_1m", 1)),
            "txn_count_5m": int(enriched_features.get("txn_count_5m", 1)),
            "txn_count_1h": int(enriched_features.get("txn_count_1h", 1)),
            "amount_sum_1h": float(enriched_features.get("amount_sum_1h", 0.0)),
            "amount_mean_prior": float(enriched_features.get("amount_mean_prior", 0.0)),
            "amount_std_prior": float(enriched_features.get("amount_std_prior", 0.0)),
            "amount_z_score": float(enriched_features.get("amount_z_score", 0.0)),
            "time_since_last_sec": float(enriched_features.get("time_since_last_sec", -1.0)),
            "geo_distance_km": float(enriched_features.get("geo_distance_km", 0.0)),
            "travel_speed_kmph": float(enriched_features.get("travel_speed_kmph", 0.0)),
            "is_international": int(enriched_features.get("is_international", 0)),
            "merchant_category_freq": float(enriched_features.get("merchant_category_freq", 0.0)),
            "hour_of_day": int(enriched_features.get("hour_of_day", 12)),
            "day_of_week": int(enriched_features.get("day_of_week", 0)),
            "is_weekend": int(enriched_features.get("is_weekend", 0)),
            "device_change_flag": int(enriched_features.get("device_change_flag", 0)),
            "micro_txn_count_10m": int(enriched_features.get("micro_txn_count_10m", 0)),
            "structuring_txn_count_24h": int(enriched_features.get("structuring_txn_count_24h", 0)),
            "prior_count": int(enriched_features.get("prior_count", 0)),
            "is_warmup": bool(enriched_features.get("is_warmup", False)),
        }

        return {
            "event_id": event_id,
            "transaction_id": event_id,
            "event_time": event_time_str,
            "customer_id": str(event.get("customer_id", "00000000-0000-0000-0000-000000000000")),
            "merchant_id": str(event.get("merchant_id", "00000000-0000-0000-0000-000000000000")),
            "amount": float(event.get("amount", 0.0)),
            "currency": str(event.get("currency", "USD")).upper(),
            "channel": str(event.get("channel", "online")).lower(),
            "category": str(
                event.get("category") or event.get("merchant_category") or "retail"
            ).lower(),
            "location": location_payload,
            "fraud_score": fraud_score,
            "rule_score": rule_res.rule_score,
            "ml_score": ml_score,
            "stat_score": stat_score,
            "z_score_signal": stat_score,
            "risk_band": risk_band,
            "risk_level": risk_band,
            "decision": decision,
            "triggered_rules": rule_res.triggered_rules,
            "reasons": rule_res.reasons,
            "model_version": self.ensemble_model.model_version,
            "shadow_model_version": self.ensemble_model.shadow_model_version,
            "shadow_ml_score": shadow_ml_score,
            "rule_version": rule_res.rule_version,
            "transformation_version": settings.transformation_version,
            "schema_version": str(event.get("schema_version", "1.0")),
            "feature_time": feature_time_str,
            "decision_time": decision_time_str,
            "processing_time_ms": elapsed_ms,
            "feature_values": feature_values_payload,
            "provenance": provenance_payload,
        }
