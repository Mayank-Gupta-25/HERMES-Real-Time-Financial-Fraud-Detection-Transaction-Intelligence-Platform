"""
End-to-End Offline ML Training, Validation PR-Threshold Selection & Evaluation (`ml/src/evaluate.py`).

Per `Lab678_21.pdf` Section 5 & Blueprint `STEP AAKARSH-MOD04-01`:
  1. Generates chronological labeled dataset using `FeatureExtractor`.
  2. Performs strict 70/15/15 chronological split (`train`, `val`, `test`).
  3. Fits `StandardScaler`, `IsolationForest`, and `XGBoost` on `train`.
  4. Selects the operating threshold on the **validation** Precision-Recall curve
     that minimizes False Positive Rate (maximizes precision/F1) subject to
     `recall >= 0.85`.
  5. Evaluates on the held-out chronological **test** split and writes
     `ml/models/v1/metrics.json` + `.joblib` model artifacts.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

from .generate_training_data import generate_training_dataset
from .preprocess import FEATURE_COLUMNS, chronological_split, preprocess_splits
from .train_isolation_forest import score_isolation_forest, train_isolation_forest
from .train_xgboost import (
    HAS_XGBOOST,
    extract_feature_importances,
    predict_xgboost_proba,
    train_xgboost_classifier,
)

ML_DIR = Path(__file__).resolve().parent.parent
DEFAULT_MODELS_DIR = ML_DIR / "models" / "v1"


def select_validation_threshold(
    y_val: np.ndarray,
    val_scores: np.ndarray,
    target_recall: float = 0.85,
) -> float:
    """
    Select the probability threshold on the validation Precision-Recall curve
    that maximizes F1 / Precision subject to `recall >= target_recall`.
    """
    precisions, recalls, thresholds = precision_recall_curve(y_val, val_scores)
    best_thresh = 0.50
    best_f1 = -1.0

    for p, r, t in zip(precisions[:-1], recalls[:-1], thresholds):
        if r >= target_recall:
            denom = p + r
            f1 = (2.0 * p * r / denom) if denom > 0 else 0.0
            if f1 > best_f1:
                best_f1 = f1
                best_thresh = float(t)

    if best_f1 < 0.0 and len(thresholds) > 0:
        # Fallback: pick threshold maximizing F1 overall
        f1s = (2.0 * precisions[:-1] * recalls[:-1]) / np.maximum(
            precisions[:-1] + recalls[:-1], 1e-9
        )
        best_idx = int(np.argmax(f1s))
        best_thresh = float(thresholds[best_idx])

    return round(float(np.clip(best_thresh, 0.15, 0.90)), 4)


def run_training_and_evaluation(
    n_samples: int = 5000,
    fraud_rate: float = 0.05,
    seed: int = 42,
    model_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """Execute full chronological training + evaluation and write `metrics.json`."""
    out_dir = Path(model_dir) if model_dir is not None else DEFAULT_MODELS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    df = generate_training_dataset(n_samples=n_samples, fraud_rate=fraud_rate, seed=seed)
    train_df, val_df, test_df = chronological_split(df, train_ratio=0.70, val_ratio=0.15)

    X_train, y_train, X_val, y_val, X_test, y_test, _scaler = preprocess_splits(
        train_df, val_df, test_df, save_dir=out_dir
    )

    if_model = train_isolation_forest(
        X_train,
        contamination=max(0.02, min(0.15, float( np.mean(y_train) ))),
        random_state=seed,
        save_dir=out_dir,
    )
    xgb_model, scale_pos_weight = train_xgboost_classifier(
        X_train,
        y_train,
        random_state=seed,
        save_dir=out_dir,
    )

    # Ensemble probabilities (85% supervised GBDT + 15% unsupervised IsolationForest)
    val_xgb = predict_xgboost_proba(xgb_model, X_val)
    val_if = score_isolation_forest(if_model, X_val)
    val_scores = 0.85 * val_xgb + 0.15 * val_if

    selected_threshold = select_validation_threshold(y_val, val_scores, target_recall=0.85)

    test_xgb = predict_xgboost_proba(xgb_model, X_test)
    test_if = score_isolation_forest(if_model, X_test)
    test_scores = 0.85 * test_xgb + 0.15 * test_if
    test_preds = (test_scores >= selected_threshold).astype(np.int32)

    auc_roc = float(roc_auc_score(y_test, test_scores))
    pr_auc = float(average_precision_score(y_test, test_scores))
    precision = float(precision_score(y_test, test_preds, zero_division=0))
    recall = float(recall_score(y_test, test_preds, zero_division=0))
    f1 = float(f1_score(y_test, test_preds, zero_division=0))

    tn, fp, fn, tp = confusion_matrix(y_test, test_preds, labels=[0, 1]).ravel()
    fpr = float(fp) / float(max(fp + tn, 1))

    feature_importances = extract_feature_importances(
        xgb_model, X_val, y_val, FEATURE_COLUMNS
    )

    metrics: Dict[str, Any] = {
        "model_version": "ensemble-v1.0.0",
        "classifier_backend": "xgboost" if HAS_XGBOOST else "hist_gradient_boosting",
        "schema_version": "1.0",
        "feature_columns": FEATURE_COLUMNS,
        "split_policy": "chronological_70_15_15",
        "train_rows": int(len(train_df)),
        "val_rows": int(len(val_df)),
        "test_rows": int(len(test_df)),
        "scale_pos_weight": float(scale_pos_weight),
        "selected_threshold": float(selected_threshold),
        "auc_roc": round(auc_roc, 4),
        "pr_auc": round(pr_auc, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
        "false_positive_rate": round(fpr, 4),
        "confusion_matrix": {
            "tn": int(tn),
            "fp": int(fp),
            "fn": int(fn),
            "tp": int(tp),
        },
        "feature_importances": feature_importances,
    }

    metrics_path = out_dir / "metrics.json"
    with metrics_path.open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    return metrics


if __name__ == "__main__":
    results = run_training_and_evaluation(n_samples=5000, fraud_rate=0.05, seed=42)
    print(json.dumps(results, indent=2))
