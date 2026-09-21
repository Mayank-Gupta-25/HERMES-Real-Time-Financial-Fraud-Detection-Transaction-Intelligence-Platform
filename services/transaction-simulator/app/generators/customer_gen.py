"""
Module 4/5 — Synthetic Customer Profile Generator

Produces realistic customer profiles that the transaction generator uses as
the "identity" side of every transaction: home location, typical spend,
preferred categories, and risk tier.
"""
from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

from faker import Faker

fake = Faker()

RISK_TIERS = ["low", "medium", "high"]
RISK_WEIGHTS = [0.80, 0.15, 0.05]

SPENDING_CATEGORIES = [
    "groceries", "electronics", "dining", "travel", "entertainment",
    "utilities", "healthcare", "clothing", "fuel", "subscriptions",
]

CARD_TYPES = ["credit", "debit", "prepaid"]

# Population centers used to seed realistic "home" coordinates.
HOME_REGIONS = [
    {"country": "US", "city": "New York", "lat": 40.7128, "lon": -74.0060},
    {"country": "US", "city": "San Francisco", "lat": 37.7749, "lon": -122.4194},
    {"country": "US", "city": "Chicago", "lat": 41.8781, "lon": -87.6298},
    {"country": "GB", "city": "London", "lat": 51.5074, "lon": -0.1278},
    {"country": "IN", "city": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"country": "IN", "city": "Bengaluru", "lat": 12.9716, "lon": 77.5946},
    {"country": "DE", "city": "Berlin", "lat": 52.5200, "lon": 13.4050},
    {"country": "SG", "city": "Singapore", "lat": 1.3521, "lon": 103.8198},
    {"country": "AU", "city": "Sydney", "lat": -33.8688, "lon": 151.2093},
    {"country": "CA", "city": "Toronto", "lat": 43.6532, "lon": -79.3832},
]


@dataclass
class Customer:
    customer_id: str
    name: str
    email: str
    age: int
    home_country: str
    home_city: str
    home_latitude: float
    home_longitude: float
    account_created_at: str
    risk_tier: str
    preferred_categories: List[str]
    avg_transaction_amount: float
    card_type: str
    is_active: bool = True

    def to_dict(self) -> dict:
        return asdict(self)


def generate_customer() -> Customer:
    region = random.choice(HOME_REGIONS)
    created = datetime.now(timezone.utc) - timedelta(days=random.randint(30, 2500))
    return Customer(
        customer_id=str(uuid.uuid4()),
        name=fake.name(),
        email=fake.email(),
        age=random.randint(18, 80),
        home_country=region["country"],
        home_city=region["city"],
        home_latitude=round(region["lat"] + random.uniform(-0.05, 0.05), 6),
        home_longitude=round(region["lon"] + random.uniform(-0.05, 0.05), 6),
        account_created_at=created.isoformat(),
        risk_tier=random.choices(RISK_TIERS, weights=RISK_WEIGHTS, k=1)[0],
        preferred_categories=random.sample(SPENDING_CATEGORIES, k=random.randint(2, 4)),
        avg_transaction_amount=round(random.lognormvariate(mu=4.0, sigma=0.8), 2),
        card_type=random.choice(CARD_TYPES),
    )


def generate_customers(count: int) -> List[Customer]:
    """Generate `count` unique synthetic customers."""
    return [generate_customer() for _ in range(count)]


def save_customers(customers: List[Customer], path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump([c.to_dict() for c in customers], f, indent=2)


def load_customers(path: str | Path) -> List[Customer]:
    with Path(path).open() as f:
        raw = json.load(f)
    return [Customer(**c) for c in raw]


if __name__ == "__main__":
    sample = generate_customers(5)
    print(json.dumps(sample[0].to_dict(), indent=2))
