"""
Module 5 — Unit tests for the Transaction Simulator (`TC-01`, `TC-02`).
Run with: pytest services/transaction-simulator/tests/ -v
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.generators.customer_gen import generate_customer, generate_customers
from app.generators.merchant_gen import generate_merchant, generate_merchants
from app.generators.transaction_gen import (
    generate_transaction,
    generate_normal_transaction,
    generate_batch,
    load_profile,
)
from app.generators.fraud_patterns import (
    inject_fraud_pattern,
    inject_structuring,
    velocity_abuse,
    amount_spike,
    geo_impossible,
    card_testing,
    structuring,
    FRAUD_PATTERNS,
)

REQUIRED_TXN_FIELDS = {
    "event_id",
    "transaction_id",
    "event_time",
    "timestamp",
    "customer_id",
    "merchant_id",
    "amount",
    "currency",
    "transaction_type",
    "channel",
    "card_type",
    "category",
    "merchant_category",
    "location",
    "device",
    "device_info",
    "is_international",
    "status",
    "schema_version",
}


class TestCustomerGenerator:
    def test_generate_customer_has_required_fields(self):
        c = generate_customer()
        assert c.customer_id
        assert 18 <= c.age <= 80
        assert c.risk_tier in {"low", "medium", "high"}

    def test_generate_customers_count_and_uniqueness(self):
        customers = generate_customers(500)
        assert len(customers) == 500
        assert len({c.customer_id for c in customers}) == 500


class TestMerchantGenerator:
    def test_generate_merchant_has_required_fields(self):
        m = generate_merchant()
        assert m.merchant_id
        assert m.category

    def test_generate_merchants_count_and_uniqueness(self):
        merchants = generate_merchants(300)
        assert len(merchants) == 300
        assert len({m.merchant_id for m in merchants}) == 300

    def test_high_risk_flag_matches_category(self):
        merchants = generate_merchants(200)
        for m in merchants:
            if m.category in {"gambling", "money_transfer", "cash_advance", "jewelry"}:
                assert m.is_high_risk is True


class TestTransactionGenerator:
    @pytest.fixture
    def population(self):
        return generate_customers(20), generate_merchants(20)

    def test_generate_transaction_matches_schema(self, population):
        customers, merchants = population
        profile = load_profile("normal")
        txn = generate_transaction(customers[0], merchants[0], profile)
        assert REQUIRED_TXN_FIELDS.issubset(txn.keys())
        assert txn["event_id"] == txn["transaction_id"]
        assert txn["event_time"] == txn["timestamp"]
        assert txn["event_time"].endswith("Z")
        assert txn["category"] == txn["merchant_category"]
        assert txn["schema_version"] == "1.0"
        assert txn["amount"] > 0
        assert txn["channel"] in {"online", "pos", "atm", "mobile"}
        assert txn["is_fraud"] is False

    def test_generate_normal_transaction_alias(self, population):
        customers, merchants = population
        txn = generate_normal_transaction(customers[0], merchants[0])
        assert txn["schema_version"] == "1.0"
        assert txn["event_time"].endswith("Z")
        assert txn["is_fraud"] is False

    def test_generate_batch_respects_count(self, population):
        customers, merchants = population
        txns = generate_batch(customers, merchants, count=100, fraud_rate=0.0)
        assert len(txns) == 100
        assert all(t["_label"] == "normal" for t in txns)
        assert all(t["schema_version"] == "1.0" for t in txns)

    def test_generate_batch_injects_fraud(self, population):
        customers, merchants = population
        txns = generate_batch(customers, merchants, count=500, fraud_rate=1.0)
        fraud_txns = [t for t in txns if t["_label"] != "normal"]
        assert len(fraud_txns) > 0
        assert all(t["is_fraud"] is True for t in fraud_txns)


class TestFraudPatterns:
    @pytest.fixture
    def population(self):
        return generate_customers(5), generate_merchants(20)

    def test_inject_fraud_pattern_returns_labeled_burst(self, population):
        customers, merchants = population
        burst = inject_fraud_pattern(customers[0], merchants)
        assert len(burst) >= 2
        assert all(t["_fraud_pattern"] in FRAUD_PATTERNS for t in burst)
        assert all(t["customer_id"] == customers[0].customer_id for t in burst)
        assert all(t["schema_version"] == "1.0" for t in burst)
        assert all(t["event_time"].endswith("Z") for t in burst)

    def test_velocity_abuse_burst_size(self, population):
        customers, merchants = population
        burst = velocity_abuse(customers[0], merchants, datetime.now(timezone.utc))
        assert 5 <= len(burst) <= 10
        assert all(t["fraud_pattern"] == "velocity_abuse" for t in burst)

    def test_amount_spike_burst(self, population):
        customers, merchants = population
        burst = amount_spike(customers[0], merchants, datetime.now(timezone.utc))
        assert len(burst) == 2
        assert burst[1]["amount"] >= 3500.0
        assert all(t["fraud_pattern"] == "amount_spike" for t in burst)

    def test_geo_impossible_produces_two_far_apart_txns(self, population):
        customers, merchants = population
        burst = geo_impossible(customers[0], merchants, datetime.now(timezone.utc))
        assert len(burst) == 2
        assert burst[0]["location"]["country"] != burst[1]["location"]["country"]

    def test_card_testing_uses_small_amounts(self, population):
        customers, merchants = population
        burst = card_testing(customers[0], merchants, datetime.now(timezone.utc))
        assert all(t["amount"] <= 5.0 for t in burst)

    def test_structuring_stays_under_threshold(self, population):
        customers, merchants = population
        burst = structuring(customers[0], merchants, datetime.now(timezone.utc))
        assert 2 <= len(burst) <= 4
        assert all(8000.0 <= t["amount"] < 10_000 for t in burst)
        assert all(t["transaction_type"] == "transfer" for t in burst)

    def test_inject_structuring_helper(self, population):
        customers, merchants = population
        burst = inject_structuring(customers[0], merchants)
        assert 2 <= len(burst) <= 4
        assert all(8000.0 <= t["amount"] <= 9999.0 for t in burst)
        assert all(t["schema_version"] == "1.0" for t in burst)
