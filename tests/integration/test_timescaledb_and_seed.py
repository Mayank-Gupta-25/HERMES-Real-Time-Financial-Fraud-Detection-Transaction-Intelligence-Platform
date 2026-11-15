"""
Integration & Schema Verification Tests for TimescaleDB Hypertables & Seed Script
Module: MOD-05 (MOD-05-FN-02)
Test IDs: TC-21
Owner: Kushagra
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
TIMESCALE_INIT_SQL = ROOT_DIR / "infrastructure" / "timescaledb" / "init.sql"
SEED_SCRIPT_PATH = ROOT_DIR / "scripts" / "seed-db.py"


def _load_seed_module():
    spec = importlib.util.spec_from_file_location("seed_db", SEED_SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_timescaledb_init_sql_hypertables_and_continuous_aggregates() -> None:
    """TC-21: Verify TimescaleDB init.sql defines hypertables, continuous aggregates, and policies."""
    assert TIMESCALE_INIT_SQL.exists(), f"Missing {TIMESCALE_INIT_SQL}"
    sql_text = TIMESCALE_INIT_SQL.read_text(encoding="utf-8")
    sql_lower = " ".join(sql_text.lower().split())

    # 1. Verify extension
    assert "create extension if not exists timescaledb" in sql_lower

    # 2. Verify all 3 hypertables and create_hypertable calls
    for hypertable in ["transaction_metrics", "category_metrics", "geo_metrics"]:
        assert f"create table if not exists {hypertable}" in sql_lower
        assert f"create_hypertable( '{hypertable}'" in sql_lower or f"create_hypertable('{hypertable}'" in sql_lower

    # 3. Verify continuous aggregates using time_bucket
    assert "cagg_tx_metrics_5m" in sql_lower
    assert "cagg_tx_metrics_1h" in sql_lower
    assert "time_bucket('5 minutes', bucket_time)" in sql_lower
    assert "time_bucket('1 hour', bucket_time)" in sql_lower
    assert "add_continuous_aggregate_policy" in sql_lower

    # 4. Verify compression (7 days) and retention (90 days) policies
    assert "add_compression_policy" in sql_lower
    assert "interval '7 days'" in sql_lower
    assert "add_retention_policy" in sql_lower
    assert "interval '90 days'" in sql_lower


def test_seed_db_deterministic_generator_in_dry_run_mode() -> None:
    """TC-21: Verify scripts/seed-db.py generates 1,000 customers, 200 merchants, and demo history deterministically."""
    assert SEED_SCRIPT_PATH.exists(), f"Missing {SEED_SCRIPT_PATH}"
    seed_mod = _load_seed_module()

    summary_1 = seed_mod.seed_databases(
        customer_count=1000,
        merchant_count=200,
        tx_count=500,
        hours=24,
        seed=42,
        seed_demo_history=True,
        dry_run=True,
    )
    summary_2 = seed_mod.seed_databases(
        customer_count=1000,
        merchant_count=200,
        tx_count=500,
        hours=24,
        seed=42,
        seed_demo_history=True,
        dry_run=True,
    )

    assert summary_1["customer_count"] == 1000
    assert summary_1["merchant_count"] == 200
    assert summary_1["transaction_count"] == 500
    assert summary_1["alert_count"] > 0
    assert summary_1["timeseries_bucket_count"] == 288  # 24h * 12 five-minute buckets

    # Verify determinism across runs with identical seed
    ds1 = summary_1["dataset"]
    ds2 = summary_2["dataset"]
    assert ds1["customers"][0]["customer_id"] == ds2["customers"][0]["customer_id"]
    assert ds1["merchants"][0]["merchant_id"] == ds2["merchants"][0]["merchant_id"]
    assert ds1["transactions"][0]["event_id"] == ds2["transactions"][0]["event_id"]
    assert ds1["transactions"][0]["amount"] == ds2["transactions"][0]["amount"]
    assert len(ds1["redis_baselines"]) > 0
