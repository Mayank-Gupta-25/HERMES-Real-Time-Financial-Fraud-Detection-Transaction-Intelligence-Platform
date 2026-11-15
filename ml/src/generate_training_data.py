"""
Synthetic Labeled Training Dataset Generator (`ml/src/generate_training_data.py`).

Generates a chronological stream of normal and fraudulent transactions and
passes every transaction sequentially through the canonical `FeatureExtractor`
(`services/transaction-processor/app/operators/feature_extractor.py`) so offline
training features match online streaming features 1-to-1 (zero training-serving skew).
"""
from __future__ import annotations

import importlib.util
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
FEAT_EXTRACTOR_PATH = (
    REPO_ROOT / "services" / "transaction-processor" / "app" / "operators" / "feature_extractor.py"
)


def _load_feature_extractor_module():
    import sys

    if "hermes_feature_extractor" in sys.modules:
        return sys.modules["hermes_feature_extractor"]
    spec = importlib.util.spec_from_file_location("hermes_feature_extractor", FEAT_EXTRACTOR_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["hermes_feature_extractor"] = mod
    spec.loader.exec_module(mod)
    return mod


_feat_mod = _load_feature_extractor_module()
FeatureExtractor = _feat_mod.FeatureExtractor
FEATURE_COLUMNS: List[str] = list(_feat_mod.FEATURE_COLUMNS)

HOME_CITIES = [
    {"country": "US", "city": "New York", "lat": 40.7128, "lon": -74.0060},
    {"country": "US", "city": "Chicago", "lat": 41.8781, "lon": -87.6298},
    {"country": "GB", "city": "London", "lat": 51.5074, "lon": -0.1278},
    {"country": "IN", "city": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"country": "DE", "city": "Berlin", "lat": 52.5200, "lon": 13.4050},
    {"country": "SG", "city": "Singapore", "lat": 1.3521, "lon": 103.8198},
]

FOREIGN_ANOMALY_CITIES = [
    {"country": "RU", "city": "Moscow", "lat": 55.7558, "lon": 37.6173},
    {"country": "NG", "city": "Lagos", "lat": 6.5244, "lon": 3.3792},
    {"country": "BR", "city": "Sao Paulo", "lat": -23.5505, "lon": -46.6333},
]

NORMAL_CATEGORIES = ["groceries", "dining", "utilities", "fuel", "clothing", "healthcare"]
HIGH_RISK_CATEGORIES = ["gambling", "money_transfer", "cash_advance", "jewelry", "electronics"]


def _format_utc_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def generate_training_dataset(
    n_samples: int = 6000,
    fraud_rate: float = 0.04,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate `n_samples` chronologically ordered transaction records with all 16
    canonical features extracted via `FeatureExtractor` (zero temporal leakage).
    """
    rng = random.Random(seed)
    np.random.seed(seed)

    n_customers = max(40, min(250, n_samples // 25))
    customers: List[Dict[str, Any]] = []
    for i in range(n_customers):
        home = HOME_CITIES[i % len(HOME_CITIES)]
        customers.append(
            {
                "customer_id": f"00000000-0000-4000-8000-{i + 1:012d}",
                "home": home,
                "base_mean": rng.uniform(35.0, 110.0),
                "base_std": rng.uniform(5.0, 15.0),
                "preferred_category": NORMAL_CATEGORIES[i % len(NORMAL_CATEGORIES)],
                "default_device": rng.choice(["mobile", "desktop"]),
            }
        )

    raw_events: List[Dict[str, Any]] = []
    current_time = datetime(2026, 11, 1, 8, 0, 0, tzinfo=timezone.utc)

    # Phase 1: Warm-up at least 6 normal transactions per customer so every customer
    # establishes a stable historical baseline prior to the main stream.
    warmup_target = min(n_customers * 6, int(n_samples * 0.30))
    for idx in range(warmup_target):
        c = customers[idx % n_customers]
        current_time += timedelta(seconds=rng.randint(25, 90))
        amt = max(8.0, round(rng.gauss(c["base_mean"], c["base_std"]), 2))
        raw_events.append(
            {
                "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                "event_time": _format_utc_z(current_time),
                "event_time_epoch": current_time.timestamp(),
                "customer_id": c["customer_id"],
                "merchant_id": f"m-norm-{idx % 40}",
                "amount": amt,
                "currency": "USD",
                "channel": "pos",
                "card_type": "credit",
                "category": c["preferred_category"],
                "location": {
                    "country": c["home"]["country"],
                    "city": c["home"]["city"],
                    "latitude": c["home"]["lat"] + rng.uniform(-0.01, 0.01),
                    "longitude": c["home"]["lon"] + rng.uniform(-0.01, 0.01),
                },
                "device": {
                    "device_id": f"dev-{c['customer_id'][-6:]}",
                    "ip_address": "198.51.100.10",
                    "device_type": c["default_device"],
                    "os": "iOS",
                },
                "is_international": False,
                "status": "pending",
                "schema_version": "1.0",
                "is_fraud": 0,
                "fraud_pattern": "normal",
            }
        )

    # Phase 2: Main chronological stream with injected fraud patterns
    patterns = ["velocity_abuse", "amount_spike", "geo_impossible", "card_testing", "structuring"]
    while len(raw_events) < n_samples:
        c = rng.choice(customers)
        current_time += timedelta(seconds=rng.randint(35, 120))

        if rng.random() < fraud_rate and (len(raw_events) + 4) <= n_samples:
            pat = rng.choice(patterns)
            alt_device = "tablet" if c["default_device"] != "tablet" else "desktop"

            if pat == "velocity_abuse":
                # Rapid burst of 5 high-velocity transactions within 20 seconds
                burst_t = current_time
                for b_idx in range(5):
                    burst_t += timedelta(seconds=rng.uniform(2.0, 4.0))
                    raw_events.append(
                        {
                            "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                            "event_time": _format_utc_z(burst_t),
                            "event_time_epoch": burst_t.timestamp(),
                            "customer_id": c["customer_id"],
                            "merchant_id": f"m-vel-{b_idx}",
                            "amount": round(c["base_mean"] * rng.uniform(5.0, 9.0), 2),
                            "currency": "USD",
                            "channel": "online",
                            "card_type": "credit",
                            "category": rng.choice(HIGH_RISK_CATEGORIES),
                            "location": {
                                "country": c["home"]["country"],
                                "city": c["home"]["city"],
                                "latitude": c["home"]["lat"] + rng.uniform(-0.02, 0.02),
                                "longitude": c["home"]["lon"] + rng.uniform(-0.02, 0.02),
                            },
                            "device": {
                                "device_id": f"dev-fraud-{b_idx}",
                                "ip_address": "203.0.113.55",
                                "device_type": alt_device,
                                "os": "Android",
                            },
                            "is_international": False,
                            "status": "pending",
                            "schema_version": "1.0",
                            "is_fraud": 1,
                            "fraud_pattern": "velocity_abuse",
                        }
                    )
                current_time = burst_t

            elif pat == "amount_spike":
                spike_amt = round(max(c["base_mean"] * rng.uniform(25.0, 45.0), 3800.0), 2)
                raw_events.append(
                    {
                        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                        "event_time": _format_utc_z(current_time),
                        "event_time_epoch": current_time.timestamp(),
                        "customer_id": c["customer_id"],
                        "merchant_id": "m-spike-99",
                        "amount": spike_amt,
                        "currency": "USD",
                        "channel": "online",
                        "card_type": "credit",
                        "category": "jewelry",
                        "location": {
                            "country": c["home"]["country"],
                            "city": c["home"]["city"],
                            "latitude": c["home"]["lat"],
                            "longitude": c["home"]["lon"],
                        },
                        "device": {
                            "device_id": "dev-spike",
                            "ip_address": "203.0.113.88",
                            "device_type": alt_device,
                            "os": "Windows",
                        },
                        "is_international": False,
                        "status": "pending",
                        "schema_version": "1.0",
                        "is_fraud": 1,
                        "fraud_pattern": "amount_spike",
                    }
                )

            elif pat == "geo_impossible":
                # Anchor normal home transaction followed 2 minutes later by foreign transaction
                t_anchor = current_time
                raw_events.append(
                    {
                        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                        "event_time": _format_utc_z(t_anchor),
                        "event_time_epoch": t_anchor.timestamp(),
                        "customer_id": c["customer_id"],
                        "merchant_id": "m-home-anchor",
                        "amount": round(c["base_mean"], 2),
                        "currency": "USD",
                        "channel": "pos",
                        "card_type": "credit",
                        "category": c["preferred_category"],
                        "location": {
                            "country": c["home"]["country"],
                            "city": c["home"]["city"],
                            "latitude": c["home"]["lat"],
                            "longitude": c["home"]["lon"],
                        },
                        "device": {
                            "device_id": f"dev-{c['customer_id'][-6:]}",
                            "ip_address": "198.51.100.10",
                            "device_type": c["default_device"],
                            "os": "iOS",
                        },
                        "is_international": False,
                        "status": "pending",
                        "schema_version": "1.0",
                        "is_fraud": 0,
                        "fraud_pattern": "normal",
                    }
                )
                far = rng.choice(FOREIGN_ANOMALY_CITIES)
                t_far = t_anchor + timedelta(seconds=rng.randint(90, 240))
                raw_events.append(
                    {
                        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                        "event_time": _format_utc_z(t_far),
                        "event_time_epoch": t_far.timestamp(),
                        "customer_id": c["customer_id"],
                        "merchant_id": "m-foreign-geo",
                        "amount": round(c["base_mean"] * rng.uniform(12.0, 25.0), 2),
                        "currency": "USD",
                        "channel": "online",
                        "card_type": "credit",
                        "category": "money_transfer",
                        "location": {
                            "country": far["country"],
                            "city": far["city"],
                            "latitude": far["lat"],
                            "longitude": far["lon"],
                        },
                        "device": {
                            "device_id": "dev-foreign-1",
                            "ip_address": "203.0.113.199",
                            "device_type": alt_device,
                            "os": "Linux",
                        },
                        "is_international": True,
                        "status": "pending",
                        "schema_version": "1.0",
                        "is_fraud": 1,
                        "fraud_pattern": "geo_impossible",
                    }
                )
                current_time = t_far

            elif pat == "card_testing":
                burst_t = current_time
                for b_idx in range(5):
                    burst_t += timedelta(seconds=rng.uniform(1.5, 3.5))
                    raw_events.append(
                        {
                            "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                            "event_time": _format_utc_z(burst_t),
                            "event_time_epoch": burst_t.timestamp(),
                            "customer_id": c["customer_id"],
                            "merchant_id": f"m-micro-{b_idx}",
                            "amount": round(rng.uniform(0.75, 4.25), 2),
                            "currency": "USD",
                            "channel": "online",
                            "card_type": "credit",
                            "category": "gambling",
                            "location": {
                                "country": "RU",
                                "city": "Moscow",
                                "latitude": 55.7558,
                                "longitude": 37.6173,
                            },
                            "device": {
                                "device_id": f"dev-bot-{b_idx}",
                                "ip_address": "203.0.113.210",
                                "device_type": alt_device,
                                "os": "Linux",
                            },
                            "is_international": True,
                            "status": "pending",
                            "schema_version": "1.0",
                            "is_fraud": 1,
                            "fraud_pattern": "card_testing",
                        }
                    )
                current_time = burst_t

            else:  # structuring ($8,000-$9,999)
                burst_t = current_time
                for b_idx in range(2):
                    burst_t += timedelta(seconds=rng.uniform(10.0, 25.0))
                    raw_events.append(
                        {
                            "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                            "event_time": _format_utc_z(burst_t),
                            "event_time_epoch": burst_t.timestamp(),
                            "customer_id": c["customer_id"],
                            "merchant_id": f"m-struct-{b_idx}",
                            "amount": round(rng.uniform(8300.0, 9850.0), 2),
                            "currency": "USD",
                            "channel": "online",
                            "card_type": "credit",
                            "category": "money_transfer",
                            "location": {
                                "country": c["home"]["country"],
                                "city": c["home"]["city"],
                                "latitude": c["home"]["lat"],
                                "longitude": c["home"]["lon"],
                            },
                            "device": {
                                "device_id": "dev-struct",
                                "ip_address": "203.0.113.250",
                                "device_type": alt_device,
                                "os": "Windows",
                            },
                            "is_international": False,
                            "status": "pending",
                            "schema_version": "1.0",
                            "is_fraud": 1,
                            "fraud_pattern": "structuring",
                        }
                    )
                current_time = burst_t

        else:
            # Normal everyday transaction near customer's home and baseline spend
            amt = max(5.0, round(rng.gauss(c["base_mean"], c["base_std"]), 2))
            raw_events.append(
                {
                    "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                    "event_time": _format_utc_z(current_time),
                    "event_time_epoch": current_time.timestamp(),
                    "customer_id": c["customer_id"],
                    "merchant_id": f"m-norm-{rng.randint(1, 50)}",
                    "amount": amt,
                    "currency": "USD",
                    "channel": rng.choice(["pos", "online", "mobile"]),
                    "card_type": "credit",
                    "category": c["preferred_category"],
                    "location": {
                        "country": c["home"]["country"],
                        "city": c["home"]["city"],
                        "latitude": c["home"]["lat"] + rng.uniform(-0.01, 0.01),
                        "longitude": c["home"]["lon"] + rng.uniform(-0.01, 0.01),
                    },
                    "device": {
                        "device_id": f"dev-{c['customer_id'][-6:]}",
                        "ip_address": "198.51.100.10",
                        "device_type": c["default_device"],
                        "os": "iOS",
                    },
                    "is_international": False,
                    "status": "pending",
                    "schema_version": "1.0",
                    "is_fraud": 0,
                    "fraud_pattern": "normal",
                }
            )

    raw_events = raw_events[:n_samples]
    raw_events.sort(key=lambda e: e["event_time_epoch"])

    extractor = FeatureExtractor()
    rows: List[Dict[str, Any]] = []
    for ev in raw_events:
        feats = extractor.extract_features(ev)
        row: Dict[str, Any] = {
            "event_id": ev["event_id"],
            "event_time": ev["event_time"],
            "event_time_epoch": ev["event_time_epoch"],
            "customer_id": ev["customer_id"],
            "amount": ev["amount"],
            "is_fraud": int(ev["is_fraud"]),
            "fraud_pattern": ev["fraud_pattern"],
        }
        for col in FEATURE_COLUMNS:
            row[col] = float(feats[col])
        rows.append(row)

    return pd.DataFrame(rows)


if __name__ == "__main__":
    df = generate_training_dataset(n_samples=5000)
    print(f"Generated {len(df)} rows (fraud rate: {df['is_fraud'].mean():.2%})")
