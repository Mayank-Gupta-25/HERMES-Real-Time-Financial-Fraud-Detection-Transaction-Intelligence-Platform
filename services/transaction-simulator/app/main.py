"""
Module 5 — CLI entry point for the Transaction Simulator.

Examples:
    # Publish to Kafka at 5,000 TPS for 60 seconds
    python -m app.main --tps 5000 --duration 60 --fraud-rate 0.05

    # Dry run without Kafka (prints JSON lines to stdout)
    python -m app.main --tps 1000 --duration 10 --output stdout

    # Force a fresh 10K customer / 5K merchant population instead of cache
    python -m app.main --regenerate-population --tps 100 --duration 5
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from .config import Settings
from .generators.customer_gen import generate_customers, save_customers, load_customers
from .generators.merchant_gen import generate_merchants, save_merchants, load_merchants
from .generators.transaction_gen import generate_batch
from .producers.kafka_producer import SimulatorKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("hermes.simulator.main")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="HERMES Transaction Simulator")
    p.add_argument("--tps", type=int, default=100, help="Target transactions/sec (1-50000)")
    p.add_argument("--duration", type=int, default=30, help="Run duration in seconds")
    p.add_argument("--num-customers", type=int, default=10_000)
    p.add_argument("--num-merchants", type=int, default=5_000)
    p.add_argument("--fraud-rate", type=float, default=0.05)
    p.add_argument("--output", choices=["kafka", "stdout"], default="kafka")
    p.add_argument("--kafka-bootstrap-servers", default=None)
    p.add_argument("--kafka-topic", default=None)
    p.add_argument(
        "--regenerate-population",
        action="store_true",
        help="Force regeneration of customers/merchants instead of reusing data/*.json cache",
    )
    return p.parse_args()


def get_population(args: argparse.Namespace):
    customers_path = DATA_DIR / "customers.json"
    merchants_path = DATA_DIR / "merchants.json"

    if not args.regenerate_population and customers_path.exists() and merchants_path.exists():
        logger.info("Loading cached population from %s", DATA_DIR)
        customers = load_customers(customers_path)
        merchants = load_merchants(merchants_path)
    else:
        logger.info("Generating %d customers and %d merchants...", args.num_customers, args.num_merchants)
        customers = generate_customers(args.num_customers)
        merchants = generate_merchants(args.num_merchants)
        save_customers(customers, customers_path)
        save_merchants(merchants, merchants_path)
        logger.info("Saved population to %s", DATA_DIR)

    return customers, merchants


async def run_kafka(args: argparse.Namespace, settings: Settings, customers, merchants) -> None:
    producer = SimulatorKafkaProducer(
        bootstrap_servers=args.kafka_bootstrap_servers or settings.kafka_bootstrap_servers,
        topic=args.kafka_topic or settings.kafka_topic,
    )
    await producer.start()
    try:
        total_txns = max(args.tps * args.duration, 1)
        logger.info("Generating %d transactions (%d TPS x %ds)...", total_txns, args.tps, args.duration)
        txns = generate_batch(customers, merchants, total_txns, fraud_rate=args.fraud_rate)
        await producer.send_at_rate(txns, args.tps)
    finally:
        await producer.stop()


def run_stdout(args: argparse.Namespace, customers, merchants) -> None:
    total_txns = max(args.tps * args.duration, 1)
    txns = generate_batch(customers, merchants, total_txns, fraud_rate=args.fraud_rate)
    for txn in txns:
        sys.stdout.write(json.dumps(txn) + "\n")


def main() -> None:
    args = parse_args()
    settings = Settings()
    customers, merchants = get_population(args)

    if args.output == "kafka":
        asyncio.run(run_kafka(args, settings, customers, merchants))
    else:
        run_stdout(args, customers, merchants)


if __name__ == "__main__":
    main()
