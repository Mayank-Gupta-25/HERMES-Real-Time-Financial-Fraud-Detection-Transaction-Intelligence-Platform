"""
Supervised Gradient Boosted Classifier Training (`ml/src/train_xgboost.py`).

Per `Lab678_21.pdf` Section 5:
  - Computes `scale_pos_weight = count(negative) / count(positive)` on the training
    split to address severe class imbalance.
  - Uses `xgboost.XGBClassifier` if installed, else falls back cleanly to
    `sklearn.ensemble.HistGradientBoostingClassifier` with sample weights so
    real gradient-boosted tree models train and serialize on any host environment.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance

try:
    import xgboost as xgb  # type: ignore

    HAS_XGBOOST = True
except ImportError:
    xgb = None
    HAS_XGBOOST = False


def compute_scale_pos_weight(y_train: np.ndarray) -> float:
    pos = float(np.sum(y_train == 1))
    neg = float(np.sum(y_train == 0))
    if pos <= 0.0:
        return 1.0
    return round(neg / pos, 4)


def train_xgboost_classifier(
    X_train: np.ndarray,
    y_train: np.ndarray,
    random_state: int = 42,
    save_dir: Optional[Path] = None,
) -> Tuple[Any, float]:
    """
    Train XGBClassifier (or HistGradientBoostingClassifier fallback with sample_weight)
    and persist `xgboost.joblib` to `save_dir`.
    """
    scale_pos_weight = compute_scale_pos_weight(y_train)

    if HAS_XGBOOST and xgb is not None:
        model = xgb.XGBClassifier(
            n_estimators=60,
            max_depth=5,
            learning_rate=0.10,
            scale_pos_weight=scale_pos_weight,
            eval_metric="aucpr",
            random_state=random_state,
            n_jobs=1,
        )
        model.fit(X_train, y_train)
    else:
        model = HistGradientBoostingClassifier(
            max_iter=60,
            max_depth=5,
            learning_rate=0.10,
            random_state=random_state,
        )
        sample_weight = np.where(y_train == 1, scale_pos_weight, 1.0)
        model.fit(X_train, y_train, sample_weight=sample_weight)

    if save_dir is not None:
        save_dir = Path(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, save_dir / "xgboost.joblib")

    return model, scale_pos_weight


def predict_xgboost_proba(model: Any, X: np.ndarray) -> np.ndarray:
    """Return positive-class fraud probabilities in [0.0, 1.0]."""
    proba = model.predict_proba(X)
    if proba.ndim == 2 and proba.shape[1] >= 2:
        return np.clip(proba[:, 1], 0.0, 1.0)
    return np.clip(proba.ravel(), 0.0, 1.0)


def extract_feature_importances(
    model: Any,
    X_val: np.ndarray,
    y_val: np.ndarray,
    feature_names: List[str],
) -> Dict[str, float]:
    """Extract normalized feature importances for `metrics.json`."""
    if hasattr(model, "feature_importances_"):
        raw = np.asarray(model.feature_importances_, dtype=np.float64)
    else:
        perm = permutation_importance(
            model, X_val, y_val, n_repeats=3, random_state=42, n_jobs=1
        )
        raw = np.maximum(0.0, perm.importances_mean)

    total = float(np.sum(raw))
    if total > 0:
        norm = raw / total
    else:
        norm = np.ones(len(feature_names), dtype=np.float64) / float(len(feature_names))

    return {name: round(float(val), 4) for name, val in zip(feature_names, norm)}
