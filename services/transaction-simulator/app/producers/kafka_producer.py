"""
Module 5 — Async Kafka Producer for the Transaction Simulator.

Publishes generated transactions to `hermes.transactions.raw`, throttled to
hit a target TPS (1K–50K), with gzip batching and structured logging.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Iterable

from aiokafka import AIOKafkaProducer

logger = logging.getLogger("hermes.simulator.kafka_producer")


class SimulatorKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str, strip_labels: bool = True):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.strip_labels = strip_labels  # drop _label/_fraud_pattern before publishing
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            linger_ms=5,
            compression_type="gzip",
        )
        await self._producer.start()
        logger.info("Kafka producer connected to %s (topic=%s)", self.bootstrap_servers, self.topic)

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

    def _clean(self, txn: dict) -> dict:
        if not self.strip_labels:
            return txn
        return {k: v for k, v in txn.items() if not k.startswith("_")}

    async def send(self, txn: dict) -> None:
        assert self._producer is not None, "call start() before send()"
        payload = self._clean(txn)
        # Key by customer_id so all of one customer's events land on the same
        # partition — required for downstream velocity/geo checks to see
        # ordered per-customer history.
        await self._producer.send_and_wait(self.topic, value=payload, key=txn["customer_id"])

    async def send_at_rate(self, transactions: Iterable[dict], tps: int) -> dict:
        """
        Publish `transactions` throttled to `tps` transactions/second.
        Returns {sent, elapsed_seconds, achieved_tps}.
        """
        interval = 1.0 / tps if tps > 0 else 0
        sent = 0
        start = time.perf_counter()
        next_tick = start

        for txn in transactions:
            await self.send(txn)
            sent += 1

            if tps > 0 and sent % tps == 0:
                logger.info("Published %d transactions", sent)

            next_tick += interval
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

        elapsed = time.perf_counter() - start
        stats = {
            "sent": sent,
            "elapsed_seconds": round(elapsed, 2),
            "achieved_tps": round(sent / elapsed, 2) if elapsed > 0 else 0,
        }
        logger.info("Run complete: %s", stats)
        return stats
