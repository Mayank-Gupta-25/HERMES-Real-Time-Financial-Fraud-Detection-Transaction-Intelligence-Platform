"""
Unit tests for Date-Partitioned (`event_date=YYYY-MM-DD`) Snappy Parquet Archiver
and Date-Pruned Analytical Readback (`STEP AAKARSH-MOD05-01`, `TC-23`).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import uuid

import pyarrow.parquet as pq
import pytest

PROCESSOR_ROOT = Path(__file__).resolve().parent.parent


def _load_parquet_archiver():
    mod_name = "hermes_parquet_archiver"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    full_path = PROCESSOR_ROOT / "app" / "sinks" / "parquet_archiver.py"
    spec = importlib.util.spec_from_file_location(mod_name, full_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


pq_mod = _load_parquet_archiver()
ParquetArchiver = pq_mod.ParquetArchiver


def _make_scored_decision(
    event_date: str,
    index: int,
    amount: float = 125.0,
    decision: str = "approve",
    risk_band: str = "low",
    fraud_score: float = 0.12,
) -> dict:
    eid = str(uuid.uuid4())
    return {
        "event_id": eid,
        "transaction_id": eid,
        "event_time": f"{event_date}T14:{index:02d}:00.000Z",
        "customer_id": f"cust-{index:03d}",
        "merchant_id": f"merch-{index:03d}",
        "amount": amount,
        "currency": "USD",
        "channel": "online",
        "category": "electronics",
        "location": {
            "country": "US",
            "city": "New York",
            "latitude": 40.7128,
            "longitude": -74.0060,
        },
        "fraud_score": fraud_score,
        "rule_score": 0.10,
        "ml_score": 0.14,
        "stat_score": 0.08,
        "risk_band": risk_band,
        "decision": decision,
        "triggered_rules": ["GEO_IMPOSSIBILITY"] if decision == "block" else [],
        "feature_values": {"txn_count_1m": 1, "amount_z_score": 0.5},
        "provenance": {
            "source_topic": "hermes.transactions.raw",
            "partition": 0,
            "offset": index,
        },
        "model_version": "ensemble-v1.0.0",
        "rule_version": "rules-v1.0",
        "schema_version": "1.0",
    }


def test_parquet_archiver_partitions_by_event_date_only(tmp_path: Path) -> None:
    """
    TC-23: Feed 20 scored transactions across 2 dates (`2026-11-11` and `2026-11-12`),
    write Parquet files to a `tmp_path` bucket directory, and assert exact path structure
    `transactions/event_date=2026-11-11/*.parquet` and `transactions/event_date=2026-11-12/*.parquet`
    with Snappy compression and zero high-cardinality partition directories.
    """
    archiver = ParquetArchiver(archive_dir=tmp_path)

    batch = [
        _make_scored_decision("2026-11-11", i, amount=100.0) for i in range(10)
    ] + [
        _make_scored_decision(
            "2026-11-12",
            i,
            amount=250.0,
            decision="block" if i < 2 else "approve",
            risk_band="critical" if i < 2 else "low",
            fraud_score=0.92 if i < 2 else 0.15,
        )
        for i in range(10)
    ]

    written_keys = archiver.archive_batch(batch)
    assert len(written_keys) == 2
    assert any(k.startswith("transactions/event_date=2026-11-11/batch_") and k.endswith(".parquet") for k in written_keys)
    assert any(k.startswith("transactions/event_date=2026-11-12/batch_") and k.endswith(".parquet") for k in written_keys)

    tx_dir = tmp_path / "transactions"
    subdirs = sorted(p.name for p in tx_dir.iterdir() if p.is_dir())
    assert subdirs == ["event_date=2026-11-11", "event_date=2026-11-12"]

    # Verify Snappy compression on written Parquet row groups
    for key in written_keys:
        pq_path = tmp_path / key
        assert pq_path.exists()
        meta = pq.read_metadata(pq_path)
        assert meta.num_rows == 10
        col_compression = meta.row_group(0).column(0).compression
        assert col_compression.upper() == "SNAPPY"


def test_parquet_date_pruned_read_roundtrip(tmp_path: Path) -> None:
    """
    TC-23: Read back only `2026-11-12` using `read_archived_summary("2026-11-12", "2026-11-12")`
    and `read_archived_records("2026-11-12", "2026-11-12")`, verifying exact row count,
    total amount, fraud counts, and schema preservation.
    """
    archiver = ParquetArchiver(archive_dir=tmp_path)

    batch_day1 = [_make_scored_decision("2026-11-11", i, amount=50.0) for i in range(8)]
    batch_day2 = [
        _make_scored_decision(
            "2026-11-12",
            i,
            amount=200.0,
            decision="block" if i < 3 else "approve",
            risk_band="critical" if i < 3 else "low",
            fraud_score=0.90 if i < 3 else 0.10,
        )
        for i in range(12)
    ]
    archiver.archive_batch(batch_day1 + batch_day2)

    summary_day2 = archiver.read_archived_summary("2026-11-12", "2026-11-12")
    assert summary_day2["partitions_scanned"] == ["2026-11-12"]
    assert summary_day2["files_scanned"] == 1
    assert summary_day2["total_rows"] == 12
    assert summary_day2["total_amount"] == pytest.approx(2400.0)
    assert summary_day2["fraud_count"] == 3
    assert summary_day2["blocked_count"] == 3

    records_day2 = archiver.read_archived_records("2026-11-12", "2026-11-12")
    assert len(records_day2) == 12
    blocked_rows = [r for r in records_day2 if r["decision"] == "block"]
    assert len(blocked_rows) == 3
    assert blocked_rows[0]["triggered_rules"] == ["GEO_IMPOSSIBILITY"]
    assert blocked_rows[0]["schema_version"] == "1.0"
