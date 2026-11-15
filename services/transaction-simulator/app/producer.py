"""
Kafka Producer for the HERMES Transaction Simulator (`app/producer.py`).

Partitions messages on `hermes.transactions.raw` by `customer_id`, attaches
`schema-version: 1.0` headers, supports both JSON and Avro serialization via
`TransactionSerializer`, and allows injecting a mock producer (`confluent_kafka`
style `.produce()` or async `send_and_wait()`) so unit tests run 100% offline.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, Iterable, List, Optional

from .config import settings
from .serializer import TransactionSerializer, to_canonical_event

try:
    from confluent_kafka import Producer as ConfluentProducer  # type: ignore
except ImportError:
    ConfluentProducer = None

try:
    from aiokafka import AIOKafkaProducer  # type: ignore
except ImportError:
    AIOKafkaProducer = None

logger = logging.getLogger("hermes.simulator.producer")


class InMemoryMockProducer:
    """Deterministic in-memory producer fallback when no broker/library is connected."""

    def __init__(self) -> None:
        self.messages: List[Dict[str, Any]] = []
        self.flushed: int = 0

    def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        headers: Optional[List[tuple]] = None,
        on_delivery: Optional[Callable] = None,
    ) -> None:
        msg = {
            "topic": topic,
            "key": key,
            "value": value,
            "headers": headers or [],
        }
        self.messages.append(msg)
        if on_delivery is not None:
            on_delivery(None, msg)

    def poll(self, timeout: float = 0.0) -> int:
        return 0

    def flush(self, timeout: float = 5.0) -> int:
        self.flushed += 1
        return 0


class SimulatorKafkaProducer:
    """
    Unified Kafka producer supporting synchronous (`produce` / `publish_batch`)
    and asynchronous (`start` / `send` / `send_at_rate` / `stop`) workflows.
    Partitions strictly by `customer_id`.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        serialization_format: Optional[str] = None,
        strip_labels: bool = True,
        producer_client: Optional[Any] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_topic
        self.serialization_format = serialization_format or settings.serialization_format
        self.strip_labels = strip_labels
        self.serializer = TransactionSerializer(
            serialization_format=self.serialization_format,
            strip_labels=strip_labels,
        )
        self._client: Any = producer_client
        self.produced_count: int = 0
        self.delivered_count: int = 0

    def _ensure_sync_client(self) -> Any:
        if self._client is None:
            if ConfluentProducer is not None:
                self._client = ConfluentProducer(
                    {
                        "bootstrap.servers": self.bootstrap_servers,
                        "acks": "all",
                        "linger.ms": 5,
                    }
                )
            else:
                self._client = InMemoryMockProducer()
        return self._client

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is None:
            self.delivered_count += 1
        else:
            logger.error("Kafka delivery failed: %s", err)

    def _clean(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        return to_canonical_event(txn, strip_labels=self.strip_labels)

    def produce(self, txn: Dict[str, Any], topic: Optional[str] = None) -> Dict[str, Any]:
        """Publish a single transaction partitioned by `customer_id`."""
        client = self._ensure_sync_client()
        target_topic = topic or self.topic
        canonical = self._clean(txn)
        key_bytes = str(canonical["customer_id"]).encode("utf-8")
        value_bytes = self.serializer.serialize(canonical)
        headers = [
            ("schema-version", str(canonical.get("schema_version", "1.0")).encode("utf-8")),
            ("content-type", f"application/{self.serialization_format}".encode("utf-8")),
        ]

        self.produced_count += 1
        client.produce(
            topic=target_topic,
            key=key_bytes,
            value=value_bytes,
            headers=headers,
            on_delivery=self._on_delivery,
        )
        if hasattr(client, "poll"):
            client.poll(0)
        else:
            # If a simple mock without on_delivery support was passed, count delivery
            self.delivered_count = max(self.delivered_count, self.produced_count)

        return {
            "topic": target_topic,
            "key": key_bytes,
            "event_id": canonical["event_id"],
            "customer_id": canonical["customer_id"],
            "schema_version": canonical["schema_version"],
        }

    def publish_batch(self, transactions: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        """Publish a batch of transactions and flush the producer."""
        client = self._ensure_sync_client()
        for txn in transactions:
            self.produce(txn)
        if hasattr(client, "flush"):
            client.flush()
        return {
            "produced_count": self.produced_count,
            "delivered_count": self.delivered_count,
            "topic": self.topic,
        }

    async def start(self) -> None:
        if self._client is not None:
            if hasattr(self._client, "start") and asyncio.iscoroutinefunction(self._client.start):
                await self._client.start()
            return
        if AIOKafkaProducer is not None:
            self._client = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                acks="all",
                linger_ms=5,
                compression_type="gzip",
            )
            await self._client.start()
        else:
            self._client = InMemoryMockProducer()

    async def stop(self) -> None:
        if self._client is not None:
            if hasattr(self._client, "stop") and asyncio.iscoroutinefunction(self._client.stop):
                await self._client.stop()
            elif hasattr(self._client, "flush"):
                self._client.flush()

    async def send(self, txn: Dict[str, Any]) -> None:
        if self._client is None:
            self._ensure_sync_client()
        canonical = self._clean(txn)
        key_bytes = str(canonical["customer_id"]).encode("utf-8")
        value_bytes = self.serializer.serialize(canonical)
        headers = [("schema-version", str(canonical.get("schema_version", "1.0")).encode("utf-8"))]

        if hasattr(self._client, "send_and_wait") and asyncio.iscoroutinefunction(self._client.send_and_wait):
            self.produced_count += 1
            await self._client.send_and_wait(
                self.topic,
                value=value_bytes,
                key=key_bytes,
                headers=headers,
            )
            self.delivered_count += 1
        else:
            self.produce(canonical)

    async def send_at_rate(self, transactions: Iterable[Dict[str, Any]], tps: int) -> Dict[str, Any]:
        interval = 1.0 / tps if tps > 0 else 0.0
        sent = 0
        start = time.perf_counter()
        next_tick = start

        for txn in transactions:
            await self.send(txn)
            sent += 1
            next_tick += interval
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

        elapsed = time.perf_counter() - start
        return {
            "sent": sent,
            "produced_count": self.produced_count,
            "delivered_count": self.delivered_count,
            "elapsed_seconds": round(elapsed, 4),
            "achieved_tps": round(sent / elapsed, 2) if elapsed > 0 else 0.0,
        }


TransactionProducer = SimulatorKafkaProducer
