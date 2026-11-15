"""
Unsupervised Isolation Forest Anomaly Detector Training (`ml/src/train_isolation_forest.py`).

Trains `sklearn.ensemble.IsolationForest` on the 16 scaled features and saves
`ml/models/v1/isolation_forest.joblib`.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest


def train_isolation_forest(
    X_train: np.ndarray,
    contamination: float = 0.04,
    n_estimators: int = 20,
    random_state: int = 42,
    save_dir: Optional[Path] = None,
) -> IsolationForest:
    """Fit IsolationForest on training features and optionally save `.joblib` artifact."""
    model = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        random_state=random_state,
        n_jobs=1,
    )
    model.fit(X_train)

    if save_dir is not None:
        save_dir = Path(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, save_dir / "isolation_forest.joblib")

    return model


def score_isolation_forest(model: IsolationForest, X: np.ndarray) -> np.ndarray:
    """
    Convert IsolationForest decision_function scores (higher = more normal)
    into calibrated anomaly probabilities in [0.0, 1.0] (higher = more anomalous).
    """
    raw = -model.decision_function(X)  # higher means more anomalous
    # Sigmoid mapping centered around 0.0
    probs = 1.0 / (1.0 + np.exp(-4.0 * raw))
    return np.clip(probs, 0.0, 1.0)
