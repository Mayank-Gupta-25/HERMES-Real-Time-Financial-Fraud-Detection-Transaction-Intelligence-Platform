from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Request

from ..engine.ab_router import ABModelRouter
from ..engine.scorer import HybridFraudScorer


router = APIRouter(tags=["model"])


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


@router.get("/model/info")
async def get_model_info(request: Request) -> dict[str, Any]:
    """
    Return active `model_version`, shadow model status, validation metrics,
    and frozen hybrid weights (`alpha_rule`, `beta_ml`, `gamma_stat`).
    """
    scorer = _get_scorer(request)
    ab_router = _get_ab_router(request)
    info = scorer.ensemble_model.get_model_info()
    weights = scorer.weights
    return {
        "model_version": info["model_version"],
        "shadow_model_version": info["shadow_model_version"],
        "rule_version": scorer.rule_engine.rule_version,
        "transformation_version": scorer.weights.get("weights_version", "weights-v1.0"),
        "artifacts_loaded": info["artifacts_loaded"],
        "model_dir": info["model_dir"],
        "feature_columns": info["feature_columns"],
        "feature_count": info["feature_count"],
        "weights": {
            "weights_version": weights.get("weights_version", "weights-v1.0"),
            "alpha_rule": float(weights.get("alpha_rule", 0.40)),
            "beta_ml": float(weights.get("beta_ml", 0.45)),
            "gamma_stat": float(weights.get("gamma_stat", 0.15)),
            "fallback_alpha_rule": float(weights.get("fallback_alpha_rule", 0.65)),
            "fallback_gamma_stat": float(weights.get("fallback_gamma_stat", 0.35)),
            "thresholds": weights.get(
                "thresholds",
                {"low_max": 0.40, "medium_max": 0.65, "high_max": 0.85},
            ),
            "warmup_min_events": int(weights.get("warmup_min_events", 5)),
            "target_recall": float(weights.get("target_recall", 0.85)),
        },
        "metrics": info["metrics"],
        "shadow_evaluation": ab_router.get_summary_metrics(),
    }
