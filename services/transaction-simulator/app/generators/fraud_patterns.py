"""
Module 4/5 — Fraud Pattern Injection

Generates known fraud scenarios so the rest of the pipeline (Flink,
fraud-detection engine, ML training set) has real signal to detect.

Implements 5 patterns:
  - velocity_abuse   : many transactions from one customer in a short burst
  - amount_spike     : sudden abnormally high-value transaction relative to customer baseline
  - geo_impossible   : two transactions too far apart to be physically possible
  - card_testing     : many small-value transactions across many merchants
  - structuring      : multiple transactions just under an AML reporting threshold ($8,000-$9,999)

Every generated transaction emits the canonical Lab 6-7-8 TransactionEvent v1.0
fields (`event_id`, `event_time` in UTC `Z`, `category`, `device`, `schema_version="1.0"`)
alongside backward-compatible aliases (`transaction_id`, `timestamp`, `merchant_category`,
`device_info`) and offline training labels (`is_fraud`, `fraud_pattern`, `_label`, `_fraud_pattern`).
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import List, Optional

FRAUD_PATTERNS = [
    "velocity_abuse",
    "amount_spike",
    "geo_impossible",
    "card_testing",
    "structuring",
]
PATTERN_WEIGHTS = [0.30, 0.15, 0.20, 0.20, 0.15]

FAR_AWAY_REGIONS = [
    {"country": "RU", "city": "Moscow", "lat": 55.7558, "lon": 37.6173},
    {"country": "NG", "city": "Lagos", "lat": 6.5244, "lon": 3.3792},
    {"country": "BR", "city": "Sao Paulo", "lat": -23.5505, "lon": -46.6333},
    {"country": "CN", "city": "Shanghai", "lat": 31.2304, "lon": 121.4737},
]


def _format_utc_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _base_txn(
    customer,
    merchant,
    timestamp: datetime,
    amount: float,
    channel: str = "online",
    transaction_type: str = "purchase",
    label: str = "fraud",
) -> dict:
    eid = str(uuid.uuid4())
    etime = _format_utc_z(timestamp)
    device_dict = {
        "device_id": f"dev-{uuid.uuid4().hex[:12]}",
        "ip_address": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
    }
    return {
        "event_id": eid,
        "transaction_id": eid,
        "event_time": etime,
        "timestamp": etime,
        "customer_id": customer.customer_id,
        "merchant_id": merchant.merchant_id,
        "amount": round(float(amount), 2),
        "currency": "USD",
        "transaction_type": transaction_type,
        "channel": channel,
        "card_type": customer.card_type,
        "category": merchant.category,
        "merchant_category": merchant.category,
        "location": {
            "country": merchant.country,
            "city": merchant.city,
            "latitude": float(merchant.latitude),
            "longitude": float(merchant.longitude),
        },
        "device": device_dict,
        "device_info": dict(device_dict),
        "is_international": merchant.country != customer.home_country,
        "status": "pending",
        "schema_version": "1.0",
        "is_fraud": True,
        "fraud_pattern": label,
        "_label": label,
        "_fraud_pattern": label,
    }


def velocity_abuse(customer, merchants, start_time: Optional[datetime] = None) -> List[dict]:
    """5-10 transactions from the same customer within ~60 seconds."""
    start_time = start_time or datetime.now(timezone.utc)
    n = random.randint(5, 10)
    txns, t = [], start_time
    for _ in range(n):
        merchant = random.choice(merchants)
        txns.append(_base_txn(customer, merchant, t, random.uniform(20, 300), label="velocity_abuse"))
        t += timedelta(seconds=random.uniform(2, 8))
    return txns


def amount_spike(customer, merchants, start_time: Optional[datetime] = None) -> List[dict]:
    """Normal baseline warmup followed by a massive anomalous transaction spike."""
    start_time = start_time or datetime.now(timezone.utc)
    m1 = random.choice(merchants)
    m2 = random.choice(merchants)
    base_amt = max(getattr(customer, "avg_transaction_amount", 65.0), 25.0)
    spike_amt = round(max(base_amt * random.uniform(15.0, 40.0), 3500.0), 2)
    t1 = start_time
    t2 = start_time + timedelta(minutes=random.randint(3, 15))
    return [
        _base_txn(customer, m1, t1, round(base_amt * random.uniform(0.8, 1.2), 2), label="amount_spike"),
        _base_txn(customer, m2, t2, spike_amt, label="amount_spike"),
    ]


def geo_impossible(customer, merchants, start_time: Optional[datetime] = None) -> List[dict]:
    """Two transactions from the same customer, minutes apart, thousands of km apart."""
    start_time = start_time or datetime.now(timezone.utc)
    m1 = random.choice(merchants)
    candidate_regions = [r for r in FAR_AWAY_REGIONS if r["country"] != m1.country] or FAR_AWAY_REGIONS
    region = random.choice(candidate_regions)
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


def card_testing(customer, merchants, start_time: Optional[datetime] = None) -> List[dict]:
    """Many small transactions across many merchants in rapid succession —
    the classic stolen-card validation pattern."""
    start_time = start_time or datetime.now(timezone.utc)
    n = random.randint(6, 15)
    chosen = random.sample(merchants, k=min(n, len(merchants)))
    txns, t = [], start_time
    for merchant in chosen:
        txns.append(_base_txn(customer, merchant, t, random.uniform(0.5, 5.0), label="card_testing"))
        t += timedelta(seconds=random.uniform(1, 5))
    return txns


def structuring(customer, merchants, start_time: Optional[datetime] = None) -> List[dict]:
    """Multiple transfers in the $8,000-$9,999 range just under the $10,000 reporting
    threshold — an AML 'structuring'/'smurfing' indicator."""
    start_time = start_time or datetime.now(timezone.utc)
    n = random.randint(2, 4)
    txns, t = [], start_time
    for _ in range(n):
        merchant = random.choice(merchants)
        amount = random.uniform(8200.0, 9850.0)
        txns.append(
            _base_txn(
                customer,
                merchant,
                t,
                amount,
                transaction_type="transfer",
                label="structuring",
            )
        )
        t += timedelta(minutes=random.uniform(15, 120))
    return txns


def inject_structuring(customer, merchants, start_time: Optional[datetime] = None) -> List[dict]:
    """Alias for structuring($8,000-$9,999) fraud pattern injector."""
    return structuring(customer, merchants, start_time=start_time)


def inject_velocity_abuse(customer, merchants, start_time: Optional[datetime] = None) -> List[dict]:
    return velocity_abuse(customer, merchants, start_time=start_time)


def inject_amount_spike(customer, merchants, start_time: Optional[datetime] = None) -> List[dict]:
    return amount_spike(customer, merchants, start_time=start_time)


def inject_geo_impossible(customer, merchants, start_time: Optional[datetime] = None) -> List[dict]:
    return geo_impossible(customer, merchants, start_time=start_time)


def inject_card_testing(customer, merchants, start_time: Optional[datetime] = None) -> List[dict]:
    return card_testing(customer, merchants, start_time=start_time)


PATTERN_FUNCS = {
    "velocity_abuse": velocity_abuse,
    "amount_spike": amount_spike,
    "geo_impossible": geo_impossible,
    "card_testing": card_testing,
    "structuring": structuring,
}


def inject_fraud_pattern(
    customer,
    merchants,
    start_time: Optional[datetime] = None,
    pattern: Optional[str] = None,
) -> List[dict]:
    """Pick a random (or specified) fraud pattern and generate its transaction burst."""
    start_time = start_time or datetime.now(timezone.utc)
    chosen_pattern = pattern or random.choices(FRAUD_PATTERNS, weights=PATTERN_WEIGHTS, k=1)[0]
    return PATTERN_FUNCS[chosen_pattern](customer, merchants, start_time)
