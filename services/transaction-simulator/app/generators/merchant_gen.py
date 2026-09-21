"""
Module 4/5 — Synthetic Merchant Profile Generator

Produces realistic merchant profiles (name, MCC-style category, location,
typical ticket size) used as the counterparty side of every transaction.
"""
from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List

from faker import Faker

fake = Faker()

MERCHANT_CATEGORIES = [
    "electronics", "groceries", "dining", "travel", "entertainment",
    "utilities", "healthcare", "clothing", "fuel", "subscriptions",
    "jewelry", "gambling", "money_transfer", "cash_advance",
]

# Categories that are statistically over-represented in real-world fraud.
# Used only to shape realistic data — the fraud engine still has to catch it.
HIGH_RISK_CATEGORIES = {"gambling", "money_transfer", "cash_advance", "jewelry"}

REGIONS = [
    {"country": "US", "city": "New York", "lat": 40.7128, "lon": -74.0060},
    {"country": "US", "city": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
    {"country": "GB", "city": "London", "lat": 51.5074, "lon": -0.1278},
    {"country": "IN", "city": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"country": "IN", "city": "Delhi", "lat": 28.7041, "lon": 77.1025},
    {"country": "DE", "city": "Berlin", "lat": 52.5200, "lon": 13.4050},
    {"country": "SG", "city": "Singapore", "lat": 1.3521, "lon": 103.8198},
    {"country": "AU", "city": "Sydney", "lat": -33.8688, "lon": 151.2093},
]


@dataclass
class Merchant:
    merchant_id: str
    name: str
    category: str
    country: str
    city: str
    latitude: float
    longitude: float
    avg_transaction_amount: float
    is_high_risk: bool
    accepts_international: bool

    def to_dict(self) -> dict:
        return asdict(self)


def generate_merchant() -> Merchant:
    region = random.choice(REGIONS)
    category = random.choice(MERCHANT_CATEGORIES)
    return Merchant(
        merchant_id=str(uuid.uuid4()),
        name=fake.company(),
        category=category,
        country=region["country"],
        city=region["city"],
        latitude=round(region["lat"] + random.uniform(-0.1, 0.1), 6),
        longitude=round(region["lon"] + random.uniform(-0.1, 0.1), 6),
        avg_transaction_amount=round(random.lognormvariate(mu=3.8, sigma=1.0), 2),
        is_high_risk=category in HIGH_RISK_CATEGORIES,
        accepts_international=random.random() < 0.35,
    )


def generate_merchants(count: int) -> List[Merchant]:
    return [generate_merchant() for _ in range(count)]


def save_merchants(merchants: List[Merchant], path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump([m.to_dict() for m in merchants], f, indent=2)


def load_merchants(path: str | Path) -> List[Merchant]:
    with Path(path).open() as f:
        raw = json.load(f)
    return [Merchant(**m) for m in raw]


if __name__ == "__main__":
    sample = generate_merchants(5)
    print(json.dumps(sample[0].to_dict(), indent=2))
