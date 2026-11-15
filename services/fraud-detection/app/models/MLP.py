from __future__ import annotations

import math
from typing import Any


class MLPModelWrapper:
    """
    Lightweight Multi-Layer Perceptron (MLP) challenger/shadow model scorer.
    Used for non-intrusive shadow evaluation (`shadow_ml_score`) alongside the champion ensemble.
    """

    def __init__(self, version: str = "heuristic-shadow-v1.0.0") -> None:
        self.version = version

    def predict_proba(self, feature_vector: list[float], features_dict: dict[str, Any] | None = None) -> float:
        """Compute deterministic two-layer MLP activation in `[0.0, 1.0]`."""
        feat = features_dict or {}
        txn_5m = float(feat.get("txn_count_5m", feature_vector[1] if len(feature_vector) > 1 else 1.0))
        amt_sum_1h = float(
            feat.get("amount_sum_1h", feature_vector[3] if len(feature_vector) > 3 else 50.0)
        )
        z_score = float(
            feat.get("amount_z_score", feature_vector[6] if len(feature_vector) > 6 else 0.0)
        )
        if feat.get("is_warmup", False):
            z_score = 0.0
        speed = float(
            feat.get("travel_speed_kmph", feature_vector[9] if len(feature_vector) > 9 else 0.0)
        )
        is_intl = float(
            feat.get("is_international", feature_vector[10] if len(feature_vector) > 10 else 0.0)
        )
        dev_change = float(
            feat.get("device_change_flag", feature_vector[15] if len(feature_vector) > 15 else 0.0)
        )

        # Hidden layer units with tanh activation
        h1 = math.tanh(0.25 * max(0.0, txn_5m - 1.0) + 0.18 * max(0.0, z_score) - 0.8)
        h2 = math.tanh((speed / 900.0) + 0.45 * is_intl + 0.35 * dev_change - 0.6)
        h3 = math.tanh((amt_sum_1h / 10000.0) - 0.7)

        out_logit = -0.95 + 1.45 * h1 + 1.75 * h2 + 1.10 * h3
        prob = 1.0 / (1.0 + math.exp(-out_logit))
        return float(min(1.0, max(0.0, round(prob, 4))))
