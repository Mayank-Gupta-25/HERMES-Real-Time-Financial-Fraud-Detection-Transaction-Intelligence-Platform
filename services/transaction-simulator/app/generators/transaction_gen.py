"""
Module 4/5 — Core Transaction Generator

Produces realistic transactions for a (customer, merchant) pair based on a
configurable profile (profiles/normal.yaml), and orchestrates fraud-pattern
injection (profiles/fraudulent.yaml + fraud_patterns.py) at a configurable rate.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import yaml

from .fraud_patterns import inject_fraud_pattern

PROFILES_DIR = Path(__file__).resolve().parent.parent / "profiles"


def load_profile(name: str) -> dict:
    path = PROFILES_DIR / f"{name}.yaml"
    with path.open() as f:
        return yaml.safe_load(f)


def _pick_weighted(options: dict):
    values, weights = zip(*options.items())
    return random.choices(values, weights=weights, k=1)[0]


def generate_transaction(customer, merchant, profile: dict, timestamp: Optional[datetime] = None) -> dict:
    """Generate a single, non-fraud transaction consistent with `profile`."""
    timestamp = timestamp or datetime.now(timezone.utc)

    amount_cfg = profile["amount"]
    base = random.lognormvariate(mu=amount_cfg["mu"], sigma=amount_cfg["sigma"])
    amount = round(min(max(base, amount_cfg["min"]), amount_cfg["max"]), 2)

    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": timestamp.isoformat(),
        "customer_id": customer.customer_id,
        "merchant_id": merchant.merchant_id,
        "amount": amount,
        "currency": "USD",
        "transaction_type": _pick_weighted(profile["transaction_type_weights"]),
        "channel": _pick_weighted(profile["channel_weights"]),
        "card_type": customer.card_type,
        "merchant_category": merchant.category,
        "location": {
            "country": merchant.country,
            "city": merchant.city,
            "latitude": merchant.latitude,
            "longitude": merchant.longitude,
        },
        "device_info": {
            "ip_address": f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "os": random.choice(["iOS", "Android", "Windows", "macOS", "Linux"]),
        },
        "is_international": merchant.country != customer.home_country,
        "status": "pending",
        "_label": "normal",
    }


def generate_batch(
    customers: List,
    merchants: List,
    count: int,
    fraud_rate: float = 0.05,
    profile_name: str = "normal",
) -> List[dict]:
    """
    Generate `count` transactions, injecting fraud-pattern bursts for roughly
    `fraud_rate` of the customers touched. Because a fraud burst yields several
    related transactions at once, the list is trimmed back down to `count`
    at the end so callers get exactly what they asked for.
    """
    profile = load_profile(profile_name)
    txns: List[dict] = []

    while len(txns) < count:
        customer = random.choice(customers)
        if random.random() < fraud_rate:
            txns.extend(inject_fraud_pattern(customer, merchants))
        else:
            merchant = random.choice(merchants)
            txns.append(generate_transaction(customer, merchant, profile))

    return txns[:count]
