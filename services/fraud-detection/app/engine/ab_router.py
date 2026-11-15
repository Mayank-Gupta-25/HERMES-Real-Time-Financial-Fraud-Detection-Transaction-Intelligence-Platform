from __future__ import annotations

import hashlib
from typing import Any

from ..config import settings
from ..models.MLP import MLPModelWrapper


class ABModelRouter:
    """
    Shadow / Champion-Challenger A/B model evaluation router (`MOD-04-FN-04`, `TC-18`).
    Evaluates both the primary champion configuration (`ensemble-v1.0.0`) and a
    challenger/shadow configuration (`v1.1.0-shadow` / `heuristic-shadow-v1.0.0`)
    on a configurable fraction of traffic (`shadow_sample_rate = 1.0` in dev/test).

    CRITICAL INVARIANT: The shadow model is strictly observational—it records
    agreement rate, score delta (`abs(champion_score - shadow_score)`), and decision
    divergence counts without EVER altering the production `fraud_score`, `risk_band`,
    or `decision`.
    """

    def __init__(
        self,
        champion_version: str | None = None,
        shadow_version: str = "v1.1.0-shadow",
        shadow_sample_rate: float = 1.0,
        shadow_alpha_rule: float = 0.35,
        shadow_beta_ml: float = 0.50,
        shadow_gamma_stat: float = 0.15,
        shadow_flag_threshold: float = 0.60,
        shadow_block_threshold: float = 0.82,
    ) -> None:
        self.champion_version = champion_version or settings.model_version
        self.shadow_version = shadow_version
        self.shadow_sample_rate = min(1.0, max(0.0, float(shadow_sample_rate)))
        self.shadow_alpha_rule = shadow_alpha_rule
        self.shadow_beta_ml = shadow_beta_ml
        self.shadow_gamma_stat = shadow_gamma_stat
        self.shadow_flag_threshold = shadow_flag_threshold
        self.shadow_block_threshold = shadow_block_threshold
        self.shadow_mlp = MLPModelWrapper(version=self.shadow_version)

        self.total_requests: int = 0
        self.shadow_evaluated_count: int = 0
        self.decision_agreement_count: int = 0
        self.decision_divergence_count: int = 0
        self.cumulative_score_delta: float = 0.0
        self.max_score_delta: float = 0.0
        self.recent_comparisons: list[dict[str, Any]] = []

    def reset_metrics(self) -> None:
        self.total_requests = 0
        self.shadow_evaluated_count = 0
        self.decision_agreement_count = 0
        self.decision_divergence_count = 0
        self.cumulative_score_delta = 0.0
        self.max_score_delta = 0.0
        self.recent_comparisons.clear()

    def _should_sample(self, event_id: str) -> bool:
        if self.shadow_sample_rate >= 1.0:
            return True
        if self.shadow_sample_rate <= 0.0:
            return False
        digest = hashlib.md5(event_id.encode("utf-8")).digest()
        bucket = int.from_bytes(digest[:4], "big") / float(0xFFFFFFFF)
        return bucket < self.shadow_sample_rate

    def classify_shadow_decision(self, shadow_hybrid_score: float) -> tuple[str, str]:
        if shadow_hybrid_score >= self.shadow_block_threshold:
            return "critical", "block"
        if shadow_hybrid_score >= self.shadow_flag_threshold:
            return "high", "flag"
        if shadow_hybrid_score >= 0.38:
            return "medium", "approve"
        return "low", "approve"

    def evaluate_shadow(
        self,
        champion_decision_payload: dict[str, Any],
        features: dict[str, Any] | None = None,
        feature_vector: list[float] | None = None,
    ) -> dict[str, Any]:
        """
        Run non-intrusive shadow evaluation alongside an already-computed champion
        `FraudDecision` dictionary. Does NOT mutate `decision`, `risk_band`, or `fraud_score`
        on `champion_decision_payload`.
        """
        self.total_requests += 1
        event_id = str(champion_decision_payload.get("event_id", ""))

        if not self._should_sample(event_id):
            return {
                "sampled": False,
                "champion_version": self.champion_version,
                "shadow_version": self.shadow_version,
            }

        feat = features or champion_decision_payload.get("feature_values") or {}
        vec = feature_vector or []

        shadow_ml_prob = float(
            champion_decision_payload.get("shadow_ml_score")
            if champion_decision_payload.get("shadow_ml_score") is not None
            else self.shadow_mlp.predict_proba(vec, features_dict=feat)
        )
        rule_score = float(champion_decision_payload.get("rule_score", 0.0))
        stat_score = float(champion_decision_payload.get("stat_score", 0.0))

        shadow_hybrid_score = min(
            1.0,
            max(
                0.0,
                round(
                    self.shadow_alpha_rule * rule_score
                    + self.shadow_beta_ml * shadow_ml_prob
                    + self.shadow_gamma_stat * stat_score,
                    4,
                ),
            ),
        )
        shadow_band, shadow_decision = self.classify_shadow_decision(shadow_hybrid_score)

        champion_score = float(champion_decision_payload.get("fraud_score", 0.0))
        champion_decision = str(champion_decision_payload.get("decision", "approve"))
        champion_band = str(champion_decision_payload.get("risk_band", "low"))

        score_delta = round(abs(champion_score - shadow_hybrid_score), 4)
        agreed = champion_decision == shadow_decision

        self.shadow_evaluated_count += 1
        if agreed:
            self.decision_agreement_count += 1
        else:
            self.decision_divergence_count += 1

        self.cumulative_score_delta += score_delta
        if score_delta > self.max_score_delta:
            self.max_score_delta = score_delta

        comparison = {
            "sampled": True,
            "event_id": event_id,
            "champion_version": self.champion_version,
            "shadow_version": self.shadow_version,
            "champion_score": champion_score,
            "shadow_score": shadow_hybrid_score,
            "shadow_ml_score": shadow_ml_prob,
            "score_delta": score_delta,
            "champion_decision": champion_decision,
            "shadow_decision": shadow_decision,
            "champion_risk_band": champion_band,
            "shadow_risk_band": shadow_band,
            "decision_agreement": agreed,
        }
        self.recent_comparisons.append(comparison)
        if len(self.recent_comparisons) > 100:
            del self.recent_comparisons[:-100]

        return comparison

    def get_summary_metrics(self) -> dict[str, Any]:
        evaluated = self.shadow_evaluated_count
        agreement_rate = (
            round(self.decision_agreement_count / float(evaluated), 4) if evaluated > 0 else 1.0
        )
        mean_delta = (
            round(self.cumulative_score_delta / float(evaluated), 4) if evaluated > 0 else 0.0
        )
        return {
            "champion_version": self.champion_version,
            "shadow_version": self.shadow_version,
            "shadow_sample_rate": self.shadow_sample_rate,
            "total_requests": self.total_requests,
            "shadow_evaluated_count": evaluated,
            "decision_agreement_count": self.decision_agreement_count,
            "decision_divergence_count": self.decision_divergence_count,
            "agreement_rate": agreement_rate,
            "mean_score_delta": mean_delta,
            "max_score_delta": round(self.max_score_delta, 4),
        }
