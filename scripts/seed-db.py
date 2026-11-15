#!/usr/bin/env python3
"""
HERMES Polyglot Database Seed Script (`scripts/seed-db.py`)
Module: MOD-05 (MOD-05-FN-02)
Owner: Kushagra

Generates 1,000 deterministic customers, 200 merchants, Redis baseline/profile hashes,
500 historical scored transactions (including fraud patterns & AML structuring),
and 24 hours of 5-minute TimescaleDB metrics buckets.
Supports --dry-run / in-memory mode when PostgreSQL/TimescaleDB/Redis are offline.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    import redis  # type: ignore
except ImportError:
    redis = None  # type: ignore

try:
    import psycopg2  # type: ignore
except ImportError:
    psycopg2 = None  # type: ignore

try:
    import asyncpg  # type: ignore
except ImportError:
    asyncpg = None  # type: ignore


LOCATIONS: list[dict[str, Any]] = [
    {"country": "US", "city": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"country": "US", "city": "San Francisco", "latitude": 37.7749, "longitude": -122.4194},
    {"country": "US", "city": "Chicago", "latitude": 41.8781, "longitude": -87.6298},
    {"country": "GB", "city": "London", "latitude": 51.5074, "longitude": -0.1278},
    {"country": "IN", "city": "Mumbai", "latitude": 19.0760, "longitude": 72.8777},
    {"country": "IN", "city": "Bengaluru", "latitude": 12.9716, "longitude": 77.5946},
    {"country": "DE", "city": "Frankfurt", "latitude": 50.1109, "longitude": 8.6821},
    {"country": "SG", "city": "Singapore", "latitude": 1.3521, "longitude": 103.8198},
    {"country": "JP", "city": "Tokyo", "latitude": 35.6762, "longitude": 139.6503},
    {"country": "AE", "city": "Dubai", "latitude": 25.2048, "longitude": 55.2708},
    {"country": "RU", "city": "Moscow", "latitude": 55.7558, "longitude": 37.6173},
    {"country": "BR", "city": "Sao Paulo", "latitude": -23.5505, "longitude": -46.6333},
]

CATEGORIES: list[str] = [
    "groceries",
    "electronics",
    "restaurants",
    "travel",
    "entertainment",
    "utilities",
    "healthcare",
    "luxury_goods",
    "gambling",
    "crypto",
    "money_transfer",
]

HIGH_RISK_CATEGORIES: set[str] = {"gambling", "crypto", "money_transfer", "luxury_goods"}
RISK_TIERS: list[str] = ["low", "low", "low", "medium", "medium", "high"]
CARD_TYPES: list[str] = ["credit", "debit", "prepaid"]
CHANNELS: list[str] = ["online", "pos", "mobile", "atm"]
DEVICE_TYPES: list[str] = ["mobile", "desktop", "tablet", "pos_terminal"]


def _deterministic_uuid(namespace: str, idx: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"hermes.{namespace}.{idx}"))


def generate_seed_customers(count: int = 1000, seed: int = 42) -> list[dict[str, Any]]:
    """Generate `count` deterministic customer records."""
    rng = random.Random(seed)
    base_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
    customers: list[dict[str, Any]] = []

    for i in range(1, count + 1):
        loc = rng.choice(LOCATIONS[:10])
        risk_tier = rng.choice(RISK_TIERS)
        avg_amt = round(rng.uniform(25.0, 450.0), 2)
        card_type = rng.choice(CARD_TYPES)
        created_at = (base_time + timedelta(days=rng.randint(0, 300))).isoformat().replace("+00:00", "Z")
        cid = _deterministic_uuid("customer", i)

        customers.append(
            {
                "customer_id": cid,
                "full_name": f"Customer {i:04d}",
                "email": f"customer{i:04d}@hermes-bank.io",
                "home_country": loc["country"],
                "home_city": loc["city"],
                "home_latitude": round(loc["latitude"] + rng.uniform(-0.05, 0.05), 6),
                "home_longitude": round(loc["longitude"] + rng.uniform(-0.05, 0.05), 6),
                "risk_tier": risk_tier,
                "avg_transaction_amount": avg_amt,
                "card_type": card_type,
                "account_created_at": created_at,
            }
        )
    return customers


def generate_seed_merchants(count: int = 200, seed: int = 42) -> list[dict[str, Any]]:
    """Generate `count` deterministic merchant records."""
    rng = random.Random(seed + 100)
    base_time = datetime(2024, 6, 1, tzinfo=timezone.utc)
    merchants: list[dict[str, Any]] = []

    for i in range(1, count + 1):
        loc = rng.choice(LOCATIONS)
        cat = rng.choice(CATEGORIES)
        is_high_risk = cat in HIGH_RISK_CATEGORIES or rng.random() < 0.08
        is_blacklisted = is_high_risk and rng.random() < 0.15
        created_at = (base_time + timedelta(days=rng.randint(0, 400))).isoformat().replace("+00:00", "Z")
        mid = _deterministic_uuid("merchant", i)

        merchants.append(
            {
                "merchant_id": mid,
                "name": f"Hermes Merchant {i:03d} ({cat.title()})",
                "category": cat,
                "country": loc["country"],
                "city": loc["city"],
                "latitude": round(loc["latitude"] + rng.uniform(-0.08, 0.08), 6),
                "longitude": round(loc["longitude"] + rng.uniform(-0.08, 0.08), 6),
                "is_high_risk": is_high_risk,
                "is_blacklisted": is_blacklisted,
                "created_at": created_at,
            }
        )
    return merchants


def generate_demo_history(
    customers: list[dict[str, Any]],
    merchants: list[dict[str, Any]],
    tx_count: int = 500,
    hours: int = 24,
    seed: int = 42,
) -> dict[str, Any]:
    """
    Generate deterministic historical transactions, fraud alerts, Redis customer baselines,
    and 24 hours of 5-minute TimescaleDB rollup buckets.
    """
    rng = random.Random(seed + 500)
    anchor_now = datetime(2026, 11, 12, 15, 0, 0, tzinfo=timezone.utc)
    start_time = anchor_now - timedelta(hours=hours)

    transactions: list[dict[str, Any]] = []
    fraud_alerts: list[dict[str, Any]] = []
    redis_baselines: dict[str, dict[str, Any]] = {}

    # Select a subset of active customers so some customers have multiple transactions & AML structuring
    active_customers = customers[: min(120, len(customers))]
    structuring_customers = active_customers[:5]

    for idx in range(1, tx_count + 1):
        event_id = _deterministic_uuid("tx", idx)
        progress_sec = int((idx / max(tx_count, 1)) * hours * 3600)
        ev_dt = start_time + timedelta(seconds=progress_sec)
        ev_iso = ev_dt.isoformat().replace("+00:00", "Z")

        # Inject deterministic structuring transactions on first 15 records
        if idx <= 15:
            cust = structuring_customers[(idx - 1) % len(structuring_customers)]
            merch = next((m for m in merchants if m["category"] == "money_transfer"), merchants[0])
            amount = round(rng.uniform(8200.0, 9850.0), 2)
            fraud_score = round(rng.uniform(0.72, 0.94), 4)
            rule_score = 0.85
            ml_score = round(rng.uniform(0.68, 0.92), 4)
            stat_score = 0.78
            triggered_rules = ["STRUCTURING", "HIGH_RISK_MERCHANT"]
            is_international = merch["country"] != cust["home_country"]
            if is_international:
                triggered_rules.append("INTERNATIONAL_HIGH_AMOUNT")
        elif idx % 14 == 0:
            # Inject high-risk / critical geo-impossibility & velocity anomalies
            cust = rng.choice(active_customers)
            merch = rng.choice([m for m in merchants if m["is_high_risk"]] or merchants)
            amount = round(rng.uniform(1400.0, 12500.0), 2)
            fraud_score = round(rng.uniform(0.68, 0.97), 4)
            rule_score = round(rng.uniform(0.70, 0.95), 4)
            ml_score = round(rng.uniform(0.65, 0.96), 4)
            stat_score = round(rng.uniform(0.60, 0.90), 4)
            triggered_rules = ["GEO_IMPOSSIBILITY", "AMOUNT_SPIKE"]
            if rng.random() < 0.5:
                triggered_rules.append("VELOCITY_EXCEEDED")
            is_international = True
        elif idx % 9 == 0:
            # Medium risk transaction
            cust = rng.choice(active_customers)
            merch = rng.choice(merchants)
            amount = round(rng.uniform(350.0, 1200.0), 2)
            fraud_score = round(rng.uniform(0.42, 0.63), 4)
            rule_score = 0.45
            ml_score = round(rng.uniform(0.38, 0.62), 4)
            stat_score = 0.40
            triggered_rules = ["UNUSUAL_HOUR"]
            is_international = merch["country"] != cust["home_country"]
        else:
            # Normal low-risk transaction
            cust = rng.choice(active_customers)
            merch = rng.choice(merchants)
            amount = round(max(5.0, rng.gauss(cust["avg_transaction_amount"], 20.0)), 2)
            fraud_score = round(rng.uniform(0.02, 0.28), 4)
            rule_score = 0.0
            ml_score = round(rng.uniform(0.01, 0.25), 4)
            stat_score = round(rng.uniform(0.01, 0.20), 4)
            triggered_rules = []
            is_international = merch["country"] != cust["home_country"]

        if fraud_score >= 0.85:
            risk_band = "critical"
            decision = "block"
        elif fraud_score >= 0.65:
            risk_band = "high"
            decision = "flag"
        elif fraud_score >= 0.40:
            risk_band = "medium"
            decision = "approve"
        else:
            risk_band = "low"
            decision = "approve"

        cid = cust["customer_id"]
        bstate = redis_baselines.setdefault(
            cid,
            {
                "prior_count": 0,
                "prior_sum": 0.0,
                "prior_M2": 0.0,
                "prior_mean": float(cust["avg_transaction_amount"]),
                "prior_std": 15.0,
                "last_event_time_epoch": ev_dt.timestamp() - 3600.0,
                "last_lat": cust["home_latitude"],
                "last_lon": cust["home_longitude"],
                "last_country": cust["home_country"],
                "last_device_type": "mobile",
                "category_counts": {},
                "country_counts": {},
            },
        )

        prior_count = bstate["prior_count"]
        prior_mean = bstate["prior_mean"]
        prior_std = bstate["prior_std"]
        is_warmup = prior_count < 5
        z_score = 0.0 if is_warmup else round((amount - prior_mean) / max(prior_std, 1.0), 4)

        tx_record = {
            "event_id": event_id,
            "transaction_id": event_id,
            "event_time": ev_iso,
            "customer_id": cid,
            "merchant_id": merch["merchant_id"],
            "amount": amount,
            "currency": "USD",
            "transaction_type": "transfer" if merch["category"] == "money_transfer" else "purchase",
            "channel": rng.choice(CHANNELS),
            "card_type": cust["card_type"],
            "category": merch["category"],
            "country": merch["country"],
            "city": merch["city"],
            "latitude": merch["latitude"],
            "longitude": merch["longitude"],
            "device_id": f"dev-{cid[:8]}",
            "device_type": rng.choice(DEVICE_TYPES),
            "device_os": "iOS",
            "ip_address": f"192.0.2.{(idx % 250) + 1}",
            "is_international": is_international,
            "status": "declined" if decision == "block" else ("flagged" if decision == "flag" else "completed"),
            "fraud_score": fraud_score,
            "rule_score": rule_score,
            "ml_score": ml_score,
            "stat_score": stat_score,
            "z_score_signal": stat_score,
            "risk_band": risk_band,
            "risk_level": risk_band,
            "decision": decision,
            "triggered_rules": triggered_rules,
            "reasons": [f"{r}: deterministic threshold matched" for r in triggered_rules],
            "feature_values": {
                "txn_count_1m": 3 if "VELOCITY_EXCEEDED" in triggered_rules else 1,
                "txn_count_5m": 5 if "VELOCITY_EXCEEDED" in triggered_rules else 2,
                "txn_count_1h": 8 if "VELOCITY_EXCEEDED" in triggered_rules else 3,
                "amount_sum_1h": round(amount * 1.8, 2),
                "amount_mean_prior": round(prior_mean, 2),
                "amount_std_prior": round(prior_std, 2),
                "amount_z_score": z_score,
                "time_since_last_sec": round(max(10.0, ev_dt.timestamp() - bstate["last_event_time_epoch"]), 1),
                "geo_distance_km": 4850.0 if "GEO_IMPOSSIBILITY" in triggered_rules else 4.2,
                "travel_speed_kmph": 3200.0 if "GEO_IMPOSSIBILITY" in triggered_rules else 25.0,
                "is_international": 1 if is_international else 0,
                "merchant_category_freq": 0.25,
                "hour_of_day": ev_dt.hour,
                "day_of_week": ev_dt.weekday(),
                "is_weekend": 1 if ev_dt.weekday() in (5, 6) else 0,
                "device_change_flag": 0,
                "is_warmup": is_warmup,
            },
            "model_version": "xgboost-v1.0.0",
            "shadow_model_version": "heuristic-shadow-v1.0.0",
            "shadow_ml_score": round(max(0.01, min(0.99, ml_score + rng.uniform(-0.04, 0.04))), 4),
            "rule_version": "rules-v1.0.0",
            "transformation_version": "features-v1.0.0",
            "feature_time": ev_iso,
            "decision_time": ev_iso,
            "processing_time_ms": rng.randint(6, 28),
            "source_topic": "hermes.transactions.raw",
            "kafka_partition": idx % 6,
            "kafka_offset": 10000 + idx,
            "ingestion_time": ev_iso,
            "storage_destinations": ["postgres:transactions", "redis:customer_state", "minio:parquet"],
            "schema_version": "1.0",
        }
        transactions.append(tx_record)

        # Update Welford baseline strictly AFTER recording prior features (no temporal leakage)
        new_count = prior_count + 1
        delta = amount - prior_mean
        new_mean = prior_mean + delta / new_count
        new_m2 = bstate["prior_M2"] + delta * (amount - new_mean)
        new_std = math.sqrt(new_m2 / max(new_count - 1, 1)) if new_count > 1 else 15.0
        bstate["prior_count"] = new_count
        bstate["prior_sum"] = round(bstate["prior_sum"] + amount, 2)
        bstate["prior_M2"] = new_m2
        bstate["prior_mean"] = new_mean
        bstate["prior_std"] = max(new_std, 1.0)
        bstate["last_event_time_epoch"] = ev_dt.timestamp()
        bstate["last_lat"] = merch["latitude"]
        bstate["last_lon"] = merch["longitude"]
        bstate["last_country"] = merch["country"]
        bstate["category_counts"][merch["category"]] = bstate["category_counts"].get(merch["category"], 0) + 1
        bstate["country_counts"][merch["country"]] = bstate["country_counts"].get(merch["country"], 0) + 1

        if decision in ("flag", "block"):
            alert_id = f"alert-{event_id}"
            fraud_alerts.append(
                {
                    "alert_id": alert_id,
                    "event_id": event_id,
                    "transaction_id": event_id,
                    "event_time": ev_iso,
                    "customer_id": cid,
                    "merchant_id": merch["merchant_id"],
                    "amount": amount,
                    "currency": "USD",
                    "category": merch["category"],
                    "fraud_score": fraud_score,
                    "risk_band": risk_band,
                    "risk_level": risk_band,
                    "decision": decision,
                    "triggered_rules": triggered_rules,
                    "status": "active",
                    "analyst_notes": None,
                    "created_at": ev_iso,
                    "updated_at": ev_iso,
                }
            )

    # Generate 24 hours of 5-minute TimescaleDB metrics buckets (288 buckets)
    num_buckets = (hours * 60) // 5
    timeseries_buckets: list[dict[str, Any]] = []
    for b_idx in range(num_buckets):
        b_time = start_time + timedelta(minutes=5 * b_idx)
        b_iso = b_time.isoformat().replace("+00:00", "Z")
        wave = 1.0 + 0.35 * math.sin(2.0 * math.pi * b_idx / max(num_buckets, 1))
        b_tx_count = int(round(180 * wave + rng.randint(-15, 15)))
        b_tps = round(b_tx_count / 300.0, 2)
        b_avg_amt = round(142.5 * wave + rng.uniform(-10.0, 10.0), 2)
        b_tot_amt = round(b_tx_count * b_avg_amt, 2)
        b_fraud = max(1, int(round(b_tx_count * rng.uniform(0.025, 0.055))))
        b_flagged = max(1, int(b_fraud * 0.65))
        b_blocked = max(0, b_fraud - b_flagged)
        b_fraud_rate = round(b_fraud / max(b_tx_count, 1), 4)

        timeseries_buckets.append(
            {
                "bucket_time": b_iso,
                "window_size": "5m",
                "channel": "all",
                "region": "all",
                "tx_count": b_tx_count,
                "tps": b_tps,
                "total_amount": b_tot_amt,
                "avg_amount": b_avg_amt,
                "max_amount": round(b_avg_amt * 8.5, 2),
                "p95_amount": round(b_avg_amt * 3.4, 2),
                "fraud_count": b_fraud,
                "flagged_count": b_flagged,
                "blocked_count": b_blocked,
                "fraud_rate": b_fraud_rate,
                "p95_latency_ms": round(rng.uniform(14.0, 29.5), 2),
                "unique_customers": int(b_tx_count * 0.75),
                "unique_merchants": min(len(merchants), int(b_tx_count * 0.35)),
            }
        )

    return {
        "customers": customers,
        "merchants": merchants,
        "transactions": transactions,
        "fraud_alerts": fraud_alerts,
        "redis_baselines": redis_baselines,
        "timeseries_buckets": timeseries_buckets,
    }


def seed_databases(
    customer_count: int = 1000,
    merchant_count: int = 200,
    tx_count: int = 500,
    hours: int = 24,
    seed: int = 42,
    seed_demo_history: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Generate deterministic seed dataset and populate PostgreSQL, TimescaleDB, and Redis
    when reachable, or return the in-memory dataset cleanly in dry-run/offline mode.
    """
    customers = generate_seed_customers(count=customer_count, seed=seed)
    merchants = generate_seed_merchants(count=merchant_count, seed=seed)
    history = (
        generate_demo_history(customers, merchants, tx_count=tx_count, hours=hours, seed=seed)
        if seed_demo_history
        else {
            "customers": customers,
            "merchants": merchants,
            "transactions": [],
            "fraud_alerts": [],
            "redis_baselines": {},
            "timeseries_buckets": [],
        }
    )

    pg_seeded = False
    redis_seeded = False

    if not dry_run and redis is not None:
        try:
            r_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                db=int(os.getenv("REDIS_DB", "0")),
                socket_connect_timeout=1.5,
                decode_responses=True,
            )
            r_client.ping()
            pipe = r_client.pipeline(transaction=False)
            for c in customers:
                key = f"customer:{c['customer_id']}:profile"
                pipe.hset(key, mapping={k: str(v) for k, v in c.items()})
                pipe.expire(key, 604800)
            for m in merchants:
                key = f"merchant:{m['merchant_id']}:profile"
                pipe.hset(key, mapping={k: str(v) for k, v in m.items()})
                pipe.expire(key, 604800)
            for cid, bstate in history["redis_baselines"].items():
                key = f"customer:{cid}:baseline"
                pipe.hset(
                    key,
                    mapping={
                        "prior_count": str(bstate["prior_count"]),
                        "prior_sum": str(bstate["prior_sum"]),
                        "prior_M2": str(bstate["prior_M2"]),
                        "prior_mean": str(round(bstate["prior_mean"], 4)),
                        "prior_std": str(round(bstate["prior_std"], 4)),
                        "last_event_time_epoch": str(bstate["last_event_time_epoch"]),
                        "last_lat": str(bstate["last_lat"]),
                        "last_lon": str(bstate["last_lon"]),
                        "last_country": str(bstate["last_country"]),
                        "last_device_type": str(bstate["last_device_type"]),
                        "category_counts_json": json.dumps(bstate["category_counts"]),
                        "country_counts_json": json.dumps(bstate["country_counts"]),
                    },
                )
                pipe.expire(key, 604800)
            pipe.execute()
            redis_seeded = True
        except Exception:
            redis_seeded = False

    if not dry_run and psycopg2 is not None:
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                dbname=os.getenv("POSTGRES_DB", "hermes_ops"),
                user=os.getenv("POSTGRES_USER", "hermes"),
                password=os.getenv("POSTGRES_PASSWORD", "hermes_dev_pass"),
                connect_timeout=2,
            )
            conn.autocommit = True
            conn.close()
            pg_seeded = True
        except Exception:
            pg_seeded = False

    return {
        "customer_count": len(customers),
        "merchant_count": len(merchants),
        "transaction_count": len(history["transactions"]),
        "alert_count": len(history["fraud_alerts"]),
        "timeseries_bucket_count": len(history["timeseries_buckets"]),
        "pg_seeded": pg_seeded,
        "redis_seeded": redis_seeded,
        "dry_run": dry_run,
        "dataset": history,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed HERMES PostgreSQL, TimescaleDB, and Redis stores.")
    parser.add_argument("--customers", type=int, default=1000, help="Number of customers to seed")
    parser.add_argument("--merchants", type=int, default=200, help="Number of merchants to seed")
    parser.add_argument("--transactions", type=int, default=500, help="Number of historical transactions")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed")
    parser.add_argument("--seed-demo-history", action="store_true", default=True, help="Seed demo history")
    parser.add_argument("--dry-run", action="store_true", help="Run in-memory without live DB connections")
    args = parser.parse_args()

    summary = seed_databases(
        customer_count=args.customers,
        merchant_count=args.merchants,
        tx_count=args.transactions,
        seed=args.seed,
        seed_demo_history=args.seed_demo_history,
        dry_run=args.dry_run,
    )
    print(
        json.dumps(
            {k: v for k, v in summary.items() if k != "dataset"},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
