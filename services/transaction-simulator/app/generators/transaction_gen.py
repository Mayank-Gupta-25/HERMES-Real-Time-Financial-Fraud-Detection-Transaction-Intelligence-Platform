"""
Module 4/5 — Core Transaction Generator

Produces realistic transactions for a (customer, merchant) pair based on a
configurable profile (profiles/normal.yaml), and orchestrates fraud-pattern
injection (profiles/fraudulent.yaml + fraud_patterns.py) at a configurable rate.

Emits canonical Lab 6-7-8 TransactionEvent v1.0 fields (`event_id`, `event_time`
in UTC `Z`, `category`, `device`, `schema_version="1.0"`) alongside backward-compatible
aliases (`transaction_id`, `timestamp`, `merchant_category`, `device_info`).
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


def load_profile(name: str = "normal") -> dict:
    path = PROFILES_DIR / f"{name}.yaml"
    with path.open() as f:
        return yaml.safe_load(f)


def _pick_weighted(options: dict):
    values, weights = zip(*options.items())
    return random.choices(values, weights=weights, k=1)[0]


def _format_utc_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def generate_transaction(
    customer,
    merchant,
    profile: Optional[dict] = None,
    timestamp: Optional[datetime] = None,
) -> dict:
    """Generate a single, non-fraud transaction consistent with `profile` and TransactionEvent v1.0."""
    profile = profile or load_profile("normal")
    timestamp = timestamp or datetime.now(timezone.utc)

    amount_cfg = profile["amount"]
    base = random.lognormvariate(mu=amount_cfg["mu"], sigma=amount_cfg["sigma"])
    amount = round(min(max(base, amount_cfg["min"]), amount_cfg["max"]), 2)

    eid = str(uuid.uuid4())
    etime = _format_utc_z(timestamp)
    device_dict = {
        "device_id": f"dev-{uuid.uuid4().hex[:12]}",
        "ip_address": f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "os": random.choice(["iOS", "Android", "Windows", "macOS", "Linux"]),
    }

    return {
        "event_id": eid,
        "transaction_id": eid,
        "event_time": etime,
        "timestamp": etime,
        "customer_id": customer.customer_id,
        "merchant_id": merchant.merchant_id,
        "amount": amount,
        "currency": "USD",
        "transaction_type": _pick_weighted(profile["transaction_type_weights"]),
        "channel": _pick_weighted(profile["channel_weights"]),
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
        "is_fraud": False,
        "fraud_pattern": None,
        "_label": "normal",
    }


def generate_normal_transaction(
    customer,
    merchant,
    profile: Optional[dict] = None,
    timestamp: Optional[datetime] = None,
) -> dict:
    """Canonical alias for generating a single normal v1.0 transaction."""
    return generate_transaction(customer, merchant, profile=profile, timestamp=timestamp)


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
