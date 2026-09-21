"""
Module 4/5 — Fraud Pattern Injection

Generates known fraud scenarios so the rest of the pipeline (Flink,
fraud-detection engine, ML training set) has real signal to detect.

Implements 4 patterns referenced in the implementation plan:
  - velocity_abuse   : many transactions from one customer in a short burst
  - geo_impossible   : two transactions too far apart to be physically possible
  - card_testing     : many small-value transactions across many merchants
  - structuring      : multiple transactions just under an AML reporting threshold

Every generated transaction carries a `_label` / `_fraud_pattern` field for
ML training and offline evaluation. These are NOT part of the production
Kafka schema — SimulatorKafkaProducer strips them before publishing.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import List

FRAUD_PATTERNS = ["velocity_abuse", "geo_impossible", "card_testing", "structuring"]
PATTERN_WEIGHTS = [0.35, 0.25, 0.25, 0.15]  # relative likelihood when fraud triggers

FAR_AWAY_REGIONS = [
    {"country": "RU", "city": "Moscow", "lat": 55.7558, "lon": 37.6173},
    {"country": "NG", "city": "Lagos", "lat": 6.5244, "lon": 3.3792},
    {"country": "BR", "city": "Sao Paulo", "lat": -23.5505, "lon": -46.6333},
    {"country": "CN", "city": "Shanghai", "lat": 31.2304, "lon": 121.4737},
]


def _base_txn(customer, merchant, timestamp, amount, channel="online",
              transaction_type="purchase", label="fraud") -> dict:
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": timestamp.isoformat(),
        "customer_id": customer.customer_id,
        "merchant_id": merchant.merchant_id,
        "amount": round(amount, 2),
        "currency": "USD",
        "transaction_type": transaction_type,
        "channel": channel,
        "card_type": customer.card_type,
        "merchant_category": merchant.category,
        "location": {
            "country": merchant.country,
            "city": merchant.city,
            "latitude": merchant.latitude,
            "longitude": merchant.longitude,
        },
        "device_info": {
            "ip_address": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
        },
        "is_international": merchant.country != customer.home_country,
        "status": "pending",
        "_label": label,
        "_fraud_pattern": label,
    }


def velocity_abuse(customer, merchants, start_time) -> List[dict]:
    """5-10 transactions from the same customer within ~60 seconds."""
    n = random.randint(5, 10)
    txns, t = [], start_time
    for _ in range(n):
        merchant = random.choice(merchants)
        txns.append(_base_txn(customer, merchant, t, random.uniform(20, 300), label="velocity_abuse"))
        t += timedelta(seconds=random.uniform(2, 8))
    return txns


def geo_impossible(customer, merchants, start_time) -> List[dict]:
    """Two transactions from the same customer, minutes apart, thousands of km apart."""
    m1 = random.choice(merchants)
    region = random.choice(FAR_AWAY_REGIONS)
    far_merchant = SimpleNamespace(
        merchant_id=random.choice(merchants).merchant_id,
        category=random.choice(merchants).category,
        country=region["country"],
        city=region["city"],
        latitude=region["lat"],
        longitude=region["lon"],
    )
    t1 = start_time
    t2 = start_time + timedelta(minutes=random.randint(2, 20))
    return [
        _base_txn(customer, m1, t1, random.uniform(30, 500), label="geo_impossible"),
        _base_txn(customer, far_merchant, t2, random.uniform(30, 500), label="geo_impossible"),
    ]


def card_testing(customer, merchants, start_time) -> List[dict]:
    """Many small transactions across many merchants in rapid succession —
    the classic stolen-card validation pattern."""
    n = random.randint(6, 15)
    chosen = random.sample(merchants, k=min(n, len(merchants)))
    txns, t = [], start_time
    for merchant in chosen:
        txns.append(_base_txn(customer, merchant, t, random.uniform(0.5, 5.0), label="card_testing"))
        t += timedelta(seconds=random.uniform(1, 5))
    return txns


def structuring(customer, merchants, start_time) -> List[dict]:
    """Multiple transfers just under a reporting threshold (e.g. $10,000) —
    an AML "structuring"/"smurfing" indicator."""
    threshold = 10_000
    n = random.randint(3, 5)
    txns, t = [], start_time
    for _ in range(n):
        merchant = random.choice(merchants)
        amount = random.uniform(threshold * 0.90, threshold * 0.99)
        txns.append(_base_txn(customer, merchant, t, amount,
                               transaction_type="transfer", label="structuring"))
        t += timedelta(hours=random.uniform(1, 12))
    return txns


PATTERN_FUNCS = {
    "velocity_abuse": velocity_abuse,
    "geo_impossible": geo_impossible,
    "card_testing": card_testing,
    "structuring": structuring,
}


def inject_fraud_pattern(customer, merchants, start_time=None) -> List[dict]:
    """Pick a random fraud pattern (weighted) and generate its transaction burst."""
    start_time = start_time or datetime.now(timezone.utc)
    pattern = random.choices(FRAUD_PATTERNS, weights=PATTERN_WEIGHTS, k=1)[0]
    return PATTERN_FUNCS[pattern](customer, merchants, start_time)
