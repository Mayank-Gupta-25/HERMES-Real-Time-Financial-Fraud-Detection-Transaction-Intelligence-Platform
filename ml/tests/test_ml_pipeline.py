"""
Unit and ML verification tests for the HERMES Offline ML Pipeline (`MOD-04-FN-02`, `TC-16`).

Verifies:
  1. Strict chronological 70/15/15 split with zero future timestamp leakage.
  2. Training & evaluation of `IsolationForest` + `XGBoost` (or `HistGradientBoostingClassifier`
     fallback with `scale_pos_weight`) producing serialized `.joblib` models and
     `ml/models/v1/metrics.json` meeting `auc_roc >= 0.90`, `recall >= 0.80`, and
     `pr_auc >= 0.75`.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ml.src.evaluate import DEFAULT_MODELS_DIR, run_training_and_evaluation
from ml.src.generate_training_data import generate_training_dataset
from ml.src.preprocess import FEATURE_COLUMNS, chronological_split


def test_chronological_split_has_no_future_leakage():
    """TC-16: Assert max(train.event_time) <= min(val.event_time) <= min(test.event_time)."""
    df = generate_training_dataset(n_samples=1000, fraud_rate=0.05, seed=42)
    assert len(df) == 1000
    for col in FEATURE_COLUMNS:
        assert col in df.columns

    train_df, val_df, test_df = chronological_split(df, train_ratio=0.70, val_ratio=0.15)
    assert len(train_df) == 700
    assert len(val_df) == 150
    assert len(test_df) == 150

    assert train_df["event_time_epoch"].max() <= val_df["event_time_epoch"].min()
    assert val_df["event_time_epoch"].max() <= test_df["event_time_epoch"].min()
    assert train_df["event_time"].max() <= val_df["event_time"].min()
    assert val_df["event_time"].max() <= test_df["event_time"].min()


def test_train_and_evaluate_pipeline_meets_metrics_targets():
    """
    TC-16: Run the training & evaluation pipeline on a 5,000-row seeded dataset,
    verify `.joblib` artifacts and `metrics.json` are generated in `ml/models/v1/`,
    and assert `auc_roc >= 0.90`, `recall >= 0.80`, and `pr_auc >= 0.75`.
    """
    metrics = run_training_and_evaluation(
        n_samples=5000,
        fraud_rate=0.05,
        seed=42,
        model_dir=DEFAULT_MODELS_DIR,
    )

    assert (DEFAULT_MODELS_DIR / "scaler.joblib").exists()
    assert (DEFAULT_MODELS_DIR / "isolation_forest.joblib").exists()
    assert (DEFAULT_MODELS_DIR / "xgboost.joblib").exists()
    assert (DEFAULT_MODELS_DIR / "metrics.json").exists()

    with (DEFAULT_MODELS_DIR / "metrics.json").open("r", encoding="utf-8") as f:
        saved_metrics = json.load(f)

    assert saved_metrics["auc_roc"] == metrics["auc_roc"]
    assert metrics["auc_roc"] >= 0.90, f"Expected auc_roc >= 0.90, got {metrics['auc_roc']}"
    assert metrics["recall"] >= 0.80, f"Expected recall >= 0.80, got {metrics['recall']}"
    assert metrics["pr_auc"] >= 0.75, f"Expected pr_auc >= 0.75, got {metrics['pr_auc']}"
    assert len(metrics["feature_importances"]) == 16
