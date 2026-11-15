"""
Integration test for Fraud Pattern Detection, Rule Triggering, and Alert Generation.
Test ID: TC-15, TC-18, TC-19, TC-28 (Fraud Detection Pipeline)
Owner: Aarussh
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any, Dict, List
import uuid

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSOR_ROOT = REPO_ROOT / "services" / "transaction-processor"
SIMULATOR_ROOT = REPO_ROOT / "services" / "transaction-simulator"

for p in (str(PROCESSOR_ROOT), str(SIMULATOR_ROOT), str(REPO_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from services.transaction_processor.app.config import settings
from services.transaction_processor.app.main import TransactionStreamPipeline
from services.transaction_simulator.app.generators.fraud_patterns import (
    inject_card_testing,
    inject_geo_impossible,
    inject_structuring,
    inject_velocity_abuse,
)
from services.transaction_simulator.app.generators.customer_gen import Customer
from services.transaction_simulator.app.generators.merchant_gen import Merchant


def _create_mock_customer() -> Customer:
    return Customer(
        customer_id=str(uuid.uuid4()),
        name="Mock Customer",
        email="mock@example.com",
        age=30,
        home_country="US",
        home_city="New York",
        home_latitude=40.7128,
        home_longitude=-74.0060,
        account_created_at="2025-01-01T00:00:00Z",
        risk_tier="low",
        preferred_categories=["retail", "electronics"],
        avg_transaction_amount=85.0,
        card_type="credit",
        is_active=True,
    )


def _create_mock_merchants() -> List[Merchant]:
    merchants = []
    for i in range(10):
        merchants.append(
            Merchant(
                merchant_id=str(uuid.uuid4()),
                name=f"Merchant-{i}",
                category="retail" if i % 2 == 0 else "electronics",
                country="US",
                city="New York",
                latitude=40.7128,
                longitude=-74.0060,
                avg_transaction_amount=100.0,
                is_high_risk=False,
                accepts_international=True,
            )
        )
    return merchants


def test_fraud_patterns_trigger_rules_and_generate_alerts(tmp_path: Path) -> None:
    """
    TC-15, TC-18, TC-19, TC-28:
    Feeds injected fraud bursts (velocity_abuse, geo_impossible, card_testing, structuring)
    through TransactionStreamPipeline and verifies:
    1. Each pattern fires its designated rule (VELOCITY_EXCEEDED, GEO_IMPOSSIBILITY, CARD_TESTING, STRUCTURING).
    2. Decisions are classified as 'flag' or 'block' with 'high' or 'critical' risk.
    3. Structured FraudAlerts are published to hermes.alerts.fraud and persisted to fraud_alerts.
    """
    customer = _create_mock_customer()
    merchants = _create_mock_merchants()

    pipeline = TransactionStreamPipeline(checkpoint_dir=tmp_path / "checkpoints")

    # Seed customer profile
    pipeline.redis_lookup.seed_customer_profile(
        customer.customer_id,
        {
            "home_country": customer.home_country,
            "home_city": customer.home_city,
            "home_latitude": customer.home_latitude,
            "home_longitude": customer.home_longitude,
            "risk_tier": customer.risk_tier,
            "avg_transaction_amount": customer.avg_transaction_amount,
            "card_type": customer.card_type,
        },
    )

    # 1. Test Pattern: Velocity Abuse (burst of 6 rapid transactions)
    velocity_burst = inject_velocity_abuse(customer, merchants, start_time=datetime.now(timezone.utc))
    assert len(velocity_burst) >= 5

    decisions_vel, _ = pipeline.process_raw_batch(velocity_burst)
    triggered_rules_vel = {rule for d in decisions_vel for rule in d.get("triggered_rules", [])}
    assert "VELOCITY_EXCEEDED" in triggered_rules_vel

    flagged_vel = [d for d in decisions_vel if d["decision"] in ("flag", "block")]
    assert len(flagged_vel) > 0
    for d in flagged_vel:
        assert d["risk_band"] in ("high", "critical")

    # 2. Test Pattern: Geo Impossible (US event -> distant international event 3 mins later)
    geo_burst = inject_geo_impossible(customer, merchants, start_time=datetime.now(timezone.utc) + timedelta(hours=1))
    assert len(geo_burst) == 2

    decisions_geo, _ = pipeline.process_raw_batch(geo_burst)
    second_geo = decisions_geo[1]
    assert "GEO_IMPOSSIBILITY" in second_geo["triggered_rules"]
    assert second_geo["decision"] == "block"
    assert second_geo["risk_band"] == "critical"

    # 3. Test Pattern: Card Testing (micro-transactions <= $5 across merchants)
    card_test_burst = inject_card_testing(customer, merchants, start_time=datetime.now(timezone.utc) + timedelta(hours=2))
    assert len(card_test_burst) >= 4

    decisions_ct, _ = pipeline.process_raw_batch(card_test_burst)
    triggered_rules_ct = {rule for d in decisions_ct for rule in d.get("triggered_rules", [])}
    assert "CARD_TESTING" in triggered_rules_ct

    # 4. Test Pattern: AML Structuring (multiple transactions $8,000-$9,999)
    struct_burst = inject_structuring(customer, merchants, start_time=datetime.now(timezone.utc) + timedelta(hours=3))
    assert len(struct_burst) >= 2

    decisions_struct, _ = pipeline.process_raw_batch(struct_burst)
    triggered_rules_struct = {rule for d in decisions_struct for rule in d.get("triggered_rules", [])}
    assert "STRUCTURING" in triggered_rules_struct

    flagged_struct = [d for d in decisions_struct if d["decision"] in ("flag", "block")]
    assert len(flagged_struct) > 0
    assert any("STRUCTURING" in d["triggered_rules"] for d in flagged_struct)

    # 5. Verify FraudAlert generation and persistence
    alerts_published = pipeline.sink.published_by_topic[settings.topic_alerts_fraud]
    assert len(alerts_published) > 0

    for item in alerts_published:
        alert = item["payload"]
        assert alert["type"] == "FRAUD_ALERT"
        assert alert["alert_id"].startswith("alert-")
        assert alert["decision"] in ("flag", "block")
        assert alert["risk_band"] in ("high", "critical")
        assert len(alert["triggered_rules"]) > 0
        assert alert["status"] == "active"

    # Verify fraud_alerts rows in PostgreSQL sink
    assert len(pipeline.postgres_sink.alerts_by_event_id) > 0
    for alert_id, alert_row in pipeline.postgres_sink.alerts_by_event_id.items():
        assert alert_row["decision"] in ("flag", "block")
        assert alert_row["status"] == "active"
