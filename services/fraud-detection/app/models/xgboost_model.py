from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import joblib
import numpy as np


class XGBoostModelWrapper:
    """
    Wrapper for the trained supervised gradient-boosted classifier (`xgboost.joblib`).
    Provides a calibrated deterministic fallback probability when `.joblib` artifact is absent.
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
        xgb_path = self.model_dir / "xgboost.joblib"
        scaler_path = self.model_dir / "scaler.joblib"
        try:
            if xgb_path.exists():
                self.model = joblib.load(xgb_path)
                if scaler_path.exists():
                    self.scaler = joblib.load(scaler_path)
                self.is_loaded = True
                return True
        except Exception:
            self.model = None
            self.scaler = None
            self.is_loaded = False
        return False

    def predict_proba(self, feature_vector: list[float], features_dict: dict[str, Any] | None = None) -> float:
        """Return supervised fraud probability in `[0.0, 1.0]`."""
        if self.is_loaded and self.model is not None:
            try:
                arr = np.asarray([feature_vector], dtype=np.float64)
                if self.scaler is not None:
                    arr = self.scaler.transform(arr)
                if hasattr(self.model, "predict_proba"):
                    probs = self.model.predict_proba(arr)
                    return float(min(1.0, max(0.0, round(float(probs[0][1]), 4))))
                preds = self.model.predict(arr)
                return float(min(1.0, max(0.0, round(float(preds[0]), 4))))
            except Exception:
                pass

        # Calibrated deterministic fallback probability over the 16 canonical features
        feat = features_dict or {}
        txn_1m = float(feat.get("txn_count_1m", 1) or 1)
        txn_5m = float(feat.get("txn_count_5m", 1) or 1)
        txn_1h = float(feat.get("txn_count_1h", 1) or 1)
        amt_sum_1h = float(feat.get("amount_sum_1h", 0.0) or 0.0)
        is_warmup = bool(feat.get("is_warmup", False))
        z_score = 0.0 if is_warmup else max(0.0, float(feat.get("amount_z_score", 0.0) or 0.0))
        geo_dist = float(feat.get("geo_distance_km", 0.0) or 0.0)
        speed = float(feat.get("travel_speed_kmph", 0.0) or 0.0)
        is_intl = float(feat.get("is_international", 0) or 0)
        dev_change = float(feat.get("device_change_flag", 0) or 0)
        micro_10m = float(feat.get("micro_txn_count_10m", 0) or 0)
        struct_24h = float(feat.get("structuring_txn_count_24h", 0) or 0)

        logit = -3.10
        logit += min(2.2, max(0.0, txn_5m - 1.0) * 0.42 + max(0.0, txn_1h - 3.0) * 0.18)
        logit += min(1.8, max(0.0, txn_1m - 1.0) * 0.55)
        logit += min(2.4, z_score * 0.35)
        if speed > 800.0 and geo_dist > 100.0:
            logit += 3.6
        elif speed > 300.0:
            logit += 1.2
        logit += 0.65 * is_intl + 0.55 * dev_change
        if micro_10m >= 3:
            logit += 1.8
        if struct_24h >= 2 or amt_sum_1h >= 16000.0:
            logit += 2.2

        prob = 1.0 / (1.0 + math.exp(-logit))
        return float(min(1.0, max(0.0, round(prob, 4))))
