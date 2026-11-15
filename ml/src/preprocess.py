"""
Chronological Train/Validation/Test Split & Feature Scaling (`ml/src/preprocess.py`).

Per `Lab678_21.pdf` Section 5:
  - Performs strict chronological splitting (`70%` train, `15%` validation, `15%` test)
    ordered by `event_time` — never random shuffling across future timestamps.
  - Fits `StandardScaler` on the training split ONLY and persists `scaler.joblib`.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

FEATURE_COLUMNS: List[str] = [
    "txn_count_1m",           # F01
    "txn_count_5m",           # F02
    "txn_count_1h",           # F03
    "amount_sum_1h",          # F04
    "amount_mean_prior",      # F05
    "amount_std_prior",       # F06
    "amount_z_score",         # F07
    "time_since_last_sec",    # F08
    "geo_distance_km",        # F09
    "travel_speed_kmph",      # F10
    "is_international",       # F11
    "merchant_category_freq", # F12
    "hour_of_day",            # F13
    "day_of_week",            # F14
    "is_weekend",             # F15
    "device_change_flag",     # F16
]


def chronological_split(
    df: pd.DataFrame,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Order `df` chronologically by `event_time` and partition into
    `train_df` (70%), `val_df` (15%), and `test_df` (15%) with zero future leakage.
    """
    sort_col = "event_time_epoch" if "event_time_epoch" in df.columns else "event_time"
    ordered = df.sort_values(sort_col, kind="mergesort").reset_index(drop=True)

    n = len(ordered)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train_df = ordered.iloc[:train_end].copy().reset_index(drop=True)
    val_df = ordered.iloc[train_end:val_end].copy().reset_index(drop=True)
    test_df = ordered.iloc[val_end:].copy().reset_index(drop=True)
    return train_df, val_df, test_df


def preprocess_splits(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    save_dir: Optional[Path] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, StandardScaler]:
    """
    Fit `StandardScaler` strictly on `train_df[FEATURE_COLUMNS]` and transform
    `train`, `val`, and `test` splits. Saves `scaler.joblib` if `save_dir` is given.
    """
    X_train_raw = train_df[FEATURE_COLUMNS].to_numpy(dtype=np.float64)
    X_val_raw = val_df[FEATURE_COLUMNS].to_numpy(dtype=np.float64)
    X_test_raw = test_df[FEATURE_COLUMNS].to_numpy(dtype=np.float64)

    y_train = train_df["is_fraud"].to_numpy(dtype=np.int32)
    y_val = val_df["is_fraud"].to_numpy(dtype=np.int32)
    y_test = test_df["is_fraud"].to_numpy(dtype=np.int32)

    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train_raw)
    X_val = scaler.transform(X_val_raw)
    X_test = scaler.transform(X_test_raw)

    if save_dir is not None:
        save_dir = Path(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)
        joblib.dump(scaler, save_dir / "scaler.joblib")

    return X_train, y_train, X_val, y_val, X_test, y_test, scaler
