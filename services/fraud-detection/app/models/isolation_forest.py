from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import joblib
import numpy as np


class IsolationForestModelWrapper:
    """
    Wrapper for the trained Isolation Forest anomaly detector (`isolation_forest.joblib`).
    Falls back to a deterministic calibrated anomaly heuristic when `.joblib` artifact is absent.
    """

    def __init__(self, model_dir: str | Path | None = None) -> None:
        self.model_dir = Path(model_dir) if model_dir else None
        self.model: Any | None = None
        self.scaler: Any | None = None
        self.is_loaded: bool = False
        if self.model_dir:
            self.load(self.model_dir)

    def load(self, model_dir: str | Path) -> bool:
        self.model_dir = Path(model_dir)
        if_path = self.model_dir / "isolation_forest.joblib"
        scaler_path = self.model_dir / "scaler.joblib"
        try:
            if if_path.exists():
                self.model = joblib.load(if_path)
                if scaler_path.exists():
                    self.scaler = joblib.load(scaler_path)
                self.is_loaded = True
                return True
        except Exception:
            self.model = None
            self.scaler = None
            self.is_loaded = False
        return False

    def predict_score(self, feature_vector: list[float], features_dict: dict[str, Any] | None = None) -> float:
        """Return normalized anomaly score in `[0.0, 1.0]`."""
        if self.is_loaded and self.model is not None:
            try:
                arr = np.asarray([feature_vector], dtype=np.float64)
                if self.scaler is not None:
                    arr = self.scaler.transform(arr)
                if hasattr(self.model, "decision_function"):
                    # IsolationForest decision_function: negative = outlier, positive = inlier
                    raw_decision = float(self.model.decision_function(arr)[0])
                    score = 1.0 / (1.0 + math.exp(4.0 * raw_decision))
                    return float(min(1.0, max(0.0, round(score, 4))))
            except Exception:
                pass

        # Calibrated deterministic fallback anomaly score
        feat = features_dict or {}
        z_abs = abs(float(feat.get("amount_z_score", 0.0) or 0.0))
        if feat.get("is_warmup", False):
            z_abs = 0.0
        speed = float(feat.get("travel_speed_kmph", 0.0) or 0.0)
        txn_5m = float(feat.get("txn_count_5m", 1) or 1)
        amt_sum_1h = float(feat.get("amount_sum_1h", 0.0) or 0.0)

        linear = -2.4
        linear += min(2.5, 0.30 * z_abs)
        linear += min(2.5, speed / 500.0)
        linear += min(1.5, max(0.0, txn_5m - 2.0) * 0.35)
        linear += min(1.5, amt_sum_1h / 12000.0)
        prob = 1.0 / (1.0 + math.exp(-linear))
        return float(min(1.0, max(0.0, round(prob, 4))))
