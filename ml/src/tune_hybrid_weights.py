"""
Validation-Split Hybrid Weight Calibration & Freezing (`ml/src/tune_hybrid_weights.py`).

Per `Lab678_21.pdf` Section 5 & Blueprint `STEP AAKARSH-MOD04-02`:
  - Evaluates grid combinations of `(alpha_rule, beta_ml, gamma_stat)` where
    `alpha + beta + gamma = 1.0` on the chronological validation split (`val_df`).
  - Selects the weight triplet that minimizes False Positive Rate (FPR) while
    achieving `recall >= 0.85` at the `high` risk threshold (`0.65`).
  - Freezes the winning weights and validation/test metrics into
    `services/fraud-detection/app/rules/scoring_weights.json` before evaluating
    once on the held-out chronological test split.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score

from .evaluate import DEFAULT_MODELS_DIR, run_training_and_evaluation
from .generate_training_data import generate_training_dataset
from .preprocess import FEATURE_COLUMNS, chronological_split
from .train_isolation_forest import score_isolation_forest
from .train_xgboost import predict_xgboost_proba

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WEIGHTS_PATH = (
    REPO_ROOT / "services" / "fraud-detection" / "app" / "rules" / "scoring_weights.json"
)


def _compute_rule_scores_for_df(df) -> np.ndarray:
    """Compute vectorized deterministic rule scores matching the 10 YAML fraud rules."""
    scores = np.zeros(len(df), dtype=np.float64)

    vel = (df["txn_count_1h"].to_numpy() > 10) | (df["txn_count_5m"].to_numpy() >= 5)
    spike = (df["amount_z_score"].to_numpy() > 2.5) & (df["amount"].to_numpy() > 500.0)
    geo = (df["travel_speed_kmph"].to_numpy() > 800.0) & (df["geo_distance_km"].to_numpy() > 100.0)
    card_test = (df["amount"].to_numpy() <= 10.0) & (
        (df["txn_count_5m"].to_numpy() >= 3) | (df["time_since_last_sec"].to_numpy() < 30.0)
    )
    intl_high = (df["is_international"].to_numpy() == 1) & (df["amount"].to_numpy() > 2000.0)
    dev_high = (df["device_change_flag"].to_numpy() == 1) & (df["amount"].to_numpy() > 300.0)
    rapid = (
        (df["time_since_last_sec"].to_numpy() >= 0.0)
        & (df["time_since_last_sec"].to_numpy() < 30.0)
        & (df["txn_count_1m"].to_numpy() >= 1)
    )
    struct = (df["amount"].to_numpy() >= 8000.0) & (df["amount"].to_numpy() < 10000.0)

    scores += 0.45 * vel.astype(np.float64)
    scores += 0.35 * spike.astype(np.float64)
    scores += 0.65 * geo.astype(np.float64)
    scores += 0.55 * card_test.astype(np.float64)
    scores += 0.40 * intl_high.astype(np.float64)
    scores += 0.35 * dev_high.astype(np.float64)
    scores += 0.30 * rapid.astype(np.float64)
    scores += 0.60 * struct.astype(np.float64)

    high_mask = vel | spike | card_test | intl_high | dev_high | rapid
    critical_mask = geo | struct
    scores = np.where(high_mask, np.maximum(scores, 0.85), scores)
    scores = np.where(critical_mask, np.maximum(scores, 0.95), scores)
    return np.clip(scores, 0.0, 1.0)


def _compute_stat_scores_for_df(df) -> np.ndarray:
    """Compute sigmoid statistical anomaly score `1 / (1 + exp(-(|z| - 2.5)))`."""
    abs_z = np.abs(df["amount_z_score"].to_numpy(dtype=np.float64))
    sig = np.where(abs_z <= 0.0, 0.0, 1.0 / (1.0 + np.exp(-np.clip(abs_z - 2.5, -30.0, 30.0))))
    return np.clip(sig, 0.0, 1.0)


def calibrate_and_freeze_weights(
    n_samples: int = 3000,
    seed: int = 42,
    model_dir: Optional[Path] = None,
    weights_path: Optional[Path] = None,
    target_recall: float = 0.85,
) -> Dict[str, Any]:
    """
    Grid-search `(alpha_rule, beta_ml, gamma_stat)` on the chronological validation
    split (`val_df`), freeze the optimal triplet into `scoring_weights.json`, and
    report held-out test split performance.
    """
    m_dir = Path(model_dir) if model_dir is not None else DEFAULT_MODELS_DIR
    w_path = Path(weights_path) if weights_path is not None else DEFAULT_WEIGHTS_PATH

    if not (
        (m_dir / "scaler.joblib").exists()
        and (m_dir / "xgboost.joblib").exists()
        and (m_dir / "isolation_forest.joblib").exists()
    ):
        run_training_and_evaluation(n_samples=max(2000, n_samples), seed=seed, model_dir=m_dir)

    scaler = joblib.load(m_dir / "scaler.joblib")
    xgb_model = joblib.load(m_dir / "xgboost.joblib")
    if_model = joblib.load(m_dir / "isolation_forest.joblib")
    if getattr(if_model, "n_estimators", 20) > 30:
        run_training_and_evaluation(n_samples=max(2000, n_samples), seed=seed, model_dir=m_dir)
        scaler = joblib.load(m_dir / "scaler.joblib")
        xgb_model = joblib.load(m_dir / "xgboost.joblib")
        if_model = joblib.load(m_dir / "isolation_forest.joblib")

    df = generate_training_dataset(n_samples=n_samples, fraud_rate=0.05, seed=seed)
    _train_df, val_df, test_df = chronological_split(df, train_ratio=0.70, val_ratio=0.15)

    X_val = scaler.transform(val_df[FEATURE_COLUMNS].to_numpy(dtype=np.float64))
    y_val = val_df["is_fraud"].to_numpy(dtype=np.int32)
    val_rule = _compute_rule_scores_for_df(val_df)
    val_ml = 0.75 * predict_xgboost_proba(xgb_model, X_val) + 0.25 * score_isolation_forest(
        if_model, X_val
    )
    val_stat = _compute_stat_scores_for_df(val_df)

    # Generate grid candidates summing to 1.0
    candidates: List[Tuple[float, float, float]] = []
    for a_int in range(20, 65, 5):
        for b_int in range(25, 65, 5):
            g_int = 100 - a_int - b_int
            if 5 <= g_int <= 35:
                candidates.append((round(a_int / 100.0, 2), round(b_int / 100.0, 2), round(g_int / 100.0, 2)))

    prior = (0.40, 0.45, 0.15)
    best_triplet = prior
    best_key = (1.0, 0.0, 999.0)  # (fpr, -recall, l1_dist_to_prior)
    flag_threshold = 0.65

    for alpha, beta, gamma in candidates:
        hybrid = alpha * val_rule + beta * val_ml + gamma * val_stat
        preds = (hybrid >= flag_threshold).astype(np.int32)
        rec = float(recall_score(y_val, preds, zero_division=0))
        neg_mask = y_val == 0
        fp = float(np.sum((preds == 1) & neg_mask))
        tn = float(np.sum((preds == 0) & neg_mask))
        fpr = fp / max(fp + tn, 1.0)
        dist = abs(alpha - prior[0]) + abs(beta - prior[1]) + abs(gamma - prior[2])

        if rec >= target_recall:
            candidate_key = (round(fpr, 4), -round(rec, 4), round(dist, 4))
            if candidate_key < best_key:
                best_key = candidate_key
                best_triplet = (alpha, beta, gamma)

    alpha_opt, beta_opt, gamma_opt = best_triplet

    # Validation metrics with frozen triplet
    val_hybrid = alpha_opt * val_rule + beta_opt * val_ml + gamma_opt * val_stat
    val_preds = (val_hybrid >= flag_threshold).astype(np.int32)
    val_recall = float(recall_score(y_val, val_preds, zero_division=0))
    val_precision = float(precision_score(y_val, val_preds, zero_division=0))
    val_f1 = float(f1_score(y_val, val_preds, zero_division=0))
    val_fp = float(np.sum((val_preds == 1) & (y_val == 0)))
    val_tn = float(np.sum((val_preds == 0) & (y_val == 0)))
    val_fpr = val_fp / max(val_fp + val_tn, 1.0)

    # Single evaluation on held-out chronological test split
    X_test = scaler.transform(test_df[FEATURE_COLUMNS].to_numpy(dtype=np.float64))
    y_test = test_df["is_fraud"].to_numpy(dtype=np.int32)
    test_rule = _compute_rule_scores_for_df(test_df)
    test_ml = 0.75 * predict_xgboost_proba(xgb_model, X_test) + 0.25 * score_isolation_forest(
        if_model, X_test
    )
    test_stat = _compute_stat_scores_for_df(test_df)
    test_hybrid = alpha_opt * test_rule + beta_opt * test_ml + gamma_opt * test_stat
    test_preds = (test_hybrid >= flag_threshold).astype(np.int32)
    test_recall = float(recall_score(y_test, test_preds, zero_division=0))
    test_precision = float(precision_score(y_test, test_preds, zero_division=0))
    test_f1 = float(f1_score(y_test, test_preds, zero_division=0))
    test_auc = float(roc_auc_score(y_test, test_hybrid))
    test_fp = float(np.sum((test_preds == 1) & (y_test == 0)))
    test_tn = float(np.sum((test_preds == 0) & (y_test == 0)))
    test_fpr = test_fp / max(test_fp + test_tn, 1.0)

    payload: Dict[str, Any] = {
        "weights_version": "weights-v1.0",
        "alpha_rule": round(alpha_opt, 2),
        "beta_ml": round(beta_opt, 2),
        "gamma_stat": round(gamma_opt, 2),
        "fallback_alpha_rule": 0.65,
        "fallback_gamma_stat": 0.35,
        "thresholds": {
            "low_max": 0.40,
            "medium_max": 0.65,
            "high_max": 0.85,
        },
        "warmup_min_events": 5,
        "target_recall": target_recall,
        "calibration_summary": {
            "calibrated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "split_policy": "chronological_70_15_15",
            "grid_candidates_evaluated": len(candidates),
            "validation_metrics": {
                "recall": round(val_recall, 4),
                "precision": round(val_precision, 4),
                "f1_score": round(val_f1, 4),
                "false_positive_rate": round(val_fpr, 4),
            },
            "test_metrics": {
                "auc_roc": round(test_auc, 4),
                "recall": round(test_recall, 4),
                "precision": round(test_precision, 4),
                "f1_score": round(test_f1, 4),
                "false_positive_rate": round(test_fpr, 4),
            },
        },
    }

    w_path.parent.mkdir(parents=True, exist_ok=True)
    with w_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return payload


if __name__ == "__main__":
    result = calibrate_and_freeze_weights()
    print(json.dumps(result, indent=2))
