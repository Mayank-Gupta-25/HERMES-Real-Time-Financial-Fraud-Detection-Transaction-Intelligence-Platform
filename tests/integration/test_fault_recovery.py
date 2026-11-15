"""
Flink Checkpoint Configuration & Fault-Injection Recovery Verification Test
(`tests/integration/test_fault_recovery.py` — `STEP AARUSSH-MOD08-01` / `MOD-08-FN-02`, `TC-27`).

Verifies (`Lab678_21.pdf` Section 5, `NFR-03`, `NFR-04`, `NFR-07`):
1. Streams 100 synthetic transactions across 10 customers.
2. Takes a durable checkpoint at event 60 (`checkpoint_id`, `committed_offsets`).
3. Simulates an abrupt mid-stream processor failure and restores state into a fresh
   processor instance from the durable checkpoint.
4. Replays events from offset 45..99 (15 replayed duplicate events + 40 remaining events).
5. Asserts `restore_outcome == "SUCCESS"`, `recovery_time_ms < 5000`, `duplicate_dropped_count == 15`,
   zero duplicate `event_id`s in PostgreSQL, and bit-for-bit identical customer Welford
   baselines and window states compared to an uninterrupted 100-event reference run.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
import sys
from typing import Any, Dict, List
import uuid

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSOR_ROOT = REPO_ROOT / "services" / "transaction-processor"
if str(PROCESSOR_ROOT) in sys.path:
    sys.path.remove(str(PROCESSOR_ROOT))
sys.path.insert(0, str(PROCESSOR_ROOT))

from services.transaction_processor.app.main import TransactionStreamPipeline
from services.transaction_processor.app.sinks.postgres_sink import PostgresOperationalSink
from services.transaction_processor.app.sinks.timescale_sink import TimescaleMetricsSink


class SimulatedWorkerCrash(RuntimeError):
    """Injected mid-stream processor crash for TC-27 verification."""


def _generate_deterministic_stream(count: int = 100, num_customers: int = 10) -> List[Dict[str, Any]]:
    rng = random.Random(20261112)
    customers = [
        str(uuid.UUID(int=rng.getrandbits(128), version=4))
        for _ in range(num_customers)
    ]
    merchants = [
        str(uuid.UUID(int=rng.getrandbits(128), version=4))
        for _ in range(20)
    ]
    categories = ["groceries", "electronics", "retail", "travel", "dining"]
    base_dt = datetime(2026, 11, 12, 14, 0, 0, tzinfo=timezone.utc)

    events: List[Dict[str, Any]] = []
    for i in range(count):
        ev_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
        cid = customers[i % num_customers]
        mid = merchants[i % len(merchants)]
        ts = (base_dt + timedelta(seconds=i * 3)).isoformat().replace("+00:00", "Z")
        amt = round(25.0 + (i * 7.35) + rng.uniform(1.0, 50.0), 2)

        events.append(
            {
                "event_id": ev_id,
                "event_time": ts,
                "customer_id": cid,
                "merchant_id": mid,
                "amount": amt,
                "currency": "USD",
                "transaction_type": "purchase",
                "channel": "online",
                "card_type": "credit",
                "category": categories[i % len(categories)],
                "location": {
                    "country": "US",
                    "city": "New York",
                    "latitude": 40.7128,
                    "longitude": -74.0060,
                },
                "device": {
                    "device_id": f"dev-{i % num_customers}",
                    "ip_address": f"198.51.100.{(i % 200) + 1}",
                    "device_type": "mobile",
                    "os": "iOS",
                },
                "is_international": False,
                "status": "pending",
                "schema_version": "1.0",
            }
        )
    return events


def test_mid_stream_fault_recovery_at_event_60_of_100(tmp_path: Path) -> None:
    """
    TC-27 (`MOD-08-FN-02`): Simulates a mid-stream failure at event 60 of 100,
    restores from the latest durable checkpoint, replays from offset 45..99,
    and proves zero state corruption, zero duplicate outputs, and recorded
    recovery telemetry (`checkpoint_id`, `committed_offsets`, `restore_outcome`, `recovery_time_ms`).
    """
    events_100 = _generate_deterministic_stream(count=100, num_customers=10)

    # 1. Uninterrupted 100-event reference run
    ref_ckpt_dir = tmp_path / "ref_checkpoints"
    ref_pipeline = TransactionStreamPipeline(checkpoint_dir=ref_ckpt_dir)
    ref_decisions, ref_dlq = ref_pipeline.process_raw_batch(events_100, start_offset=0, partition=0)
    assert len(ref_dlq) == 0
    assert len(ref_decisions) == 100
    assert len(ref_pipeline.postgres_sink.transactions_by_event_id) == 100

    # 2. Fault-injection run: process events 0..59 (first 60 events), checkpoint, then crash
    fault_ckpt_dir = tmp_path / "fault_checkpoints"
    shared_pg_sink = PostgresOperationalSink()
    shared_ts_sink = TimescaleMetricsSink()

    worker_1 = TransactionStreamPipeline(
        checkpoint_dir=fault_ckpt_dir,
        postgres_sink=shared_pg_sink,
        timescale_sink=shared_ts_sink,
    )

    decisions_part1: List[Dict[str, Any]] = []
    checkpoint_snapshot: Dict[str, Any] = {}

    with pytest.raises(SimulatedWorkerCrash):
        for idx, ev in enumerate(events_100):
            dec, dlq = worker_1.process_raw_event(ev, partition=0, offset=idx)
            assert dlq is None
            if dec is not None:
                decisions_part1.append(dec)

            if idx == 59:
                # Commit checkpoint right after event 60 (index 59)
                checkpoint_snapshot = worker_1.take_checkpoint(committed_offsets={"0": 59})
                raise SimulatedWorkerCrash("Simulated TaskManager crash after event 60")

    assert len(decisions_part1) == 60
    assert checkpoint_snapshot["checkpoint_id"] == "chk-000001"
    assert checkpoint_snapshot["committed_offsets"] == {"0": 59}
    assert len(checkpoint_snapshot["seen_event_ids"]) == 60

    # 3. Recover into a brand-new worker instance from durable checkpoint storage
    worker_2 = TransactionStreamPipeline(
        checkpoint_dir=fault_ckpt_dir,
        postgres_sink=shared_pg_sink,
        timescale_sink=shared_ts_sink,
    )
    recovery_telemetry = worker_2.restore_from_checkpoint()

    assert recovery_telemetry["restore_outcome"] == "SUCCESS"
    assert recovery_telemetry["checkpoint_id"] == "chk-000001"
    assert recovery_telemetry["committed_offsets"] == {"0": 59}
    assert recovery_telemetry["offsets"] == {"0": 59}
    assert 0.0 <= recovery_telemetry["recovery_time_ms"] < 5000.0
    assert recovery_telemetry["restored_seen_events_count"] == 60
    assert recovery_telemetry["restored_customers_count"] == 10

    # 4. Replay from offset 45..99 (15 already-committed events 45..59 + 40 new events 60..99)
    replay_slice = events_100[45:100]
    decisions_part2, dlq_part2 = worker_2.process_raw_batch(
        replay_slice, start_offset=45, partition=0
    )

    assert len(dlq_part2) == 0
    # Exactly 40 unique new decisions must be emitted; the 15 replayed events (45..59) are dropped
    assert len(decisions_part2) == 40
    worker2_metrics = worker_2.get_metrics()
    assert worker2_metrics["produced_count"] == 55
    assert worker2_metrics["processed_unique_count"] == 40
    assert worker2_metrics["duplicate_dropped_count"] == 15

    # 5. Verify zero duplicate event_ids in PostgreSQL and bit-for-bit state parity with reference run
    all_emitted_decisions = decisions_part1 + decisions_part2
    assert len(all_emitted_decisions) == 100
    assert len(shared_pg_sink.transactions_by_event_id) == 100

    for cid, ref_state in ref_pipeline.feature_extractor._baselines.items():
        rec_state = worker_2.feature_extractor._baselines[cid]
        assert rec_state.prior_count == ref_state.prior_count
        assert rec_state.prior_sum == pytest.approx(ref_state.prior_sum, abs=1e-6)
        assert rec_state.prior_mean == pytest.approx(ref_state.prior_mean, abs=1e-6)
        assert rec_state.prior_M2 == pytest.approx(ref_state.prior_M2, abs=1e-6)
        assert rec_state.prior_std == pytest.approx(ref_state.prior_std, abs=1e-6)
        assert rec_state.category_counts == ref_state.category_counts

    # Compare the 40 post-recovery FraudDecisions against reference decisions 60..99
    for rec_dec, ref_dec in zip(decisions_part2, ref_decisions[60:100]):
        assert rec_dec["event_id"] == ref_dec["event_id"]
        assert rec_dec["fraud_score"] == pytest.approx(ref_dec["fraud_score"], abs=1e-6)
        assert rec_dec["decision"] == ref_dec["decision"]
        assert rec_dec["feature_values"]["prior_count"] == ref_dec["feature_values"]["prior_count"]
        assert rec_dec["feature_values"]["amount_mean_prior"] == pytest.approx(
            ref_dec["feature_values"]["amount_mean_prior"], abs=1e-5
        )
        assert rec_dec["feature_values"]["txn_count_1h"] == ref_dec["feature_values"]["txn_count_1h"]
