"""
Kafka Source Connector for HERMES Stream Processor (MOD-02-FN-01).

Provides both a `confluent_kafka.Consumer` wrapper and an in-memory queue fallback
so unit and integration tests run deterministically without a live broker when needed.
"""
from __future__ import annotations

from collections import deque
from typing import Any, Deque, Dict, Iterable, List, Optional

from ..config import settings

try:
    from confluent_kafka import Consumer as ConfluentConsumer  # type: ignore[import-untyped]
    HAS_CONFLUENT_KAFKA = True
except ImportError:
    ConfluentConsumer = None  # type: ignore[assignment]
    HAS_CONFLUENT_KAFKA = False


class KafkaEventSource:
    """
    Consumes raw transaction payloads from `hermes.transactions.raw` (or a configured topic).
    Supports injecting in-memory records for deterministic testing.
    """

    def __init__(
        self,
        topic: Optional[str] = None,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        in_memory_records: Optional[Iterable[Any]] = None,
        use_in_memory: Optional[bool] = None,
    ) -> None:
        self.topic = topic or settings.topic_transactions_raw
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.group_id = group_id or settings.kafka_consumer_group
        self._buffer: Deque[Dict[str, Any]] = deque()
        self._consumer: Any = None
        self.committed_offsets: Dict[int, int] = {}

        if in_memory_records is not None:
            for idx, item in enumerate(in_memory_records):
                self._buffer.append(
                    {
                        "topic": self.topic,
                        "partition": idx % 6,
                        "offset": idx,
                        "value": item,
                    }
                )

        if use_in_memory is None:
            self._use_in_memory = (in_memory_records is not None) or (not HAS_CONFLUENT_KAFKA)
        else:
            self._use_in_memory = use_in_memory

        if not self._use_in_memory and HAS_CONFLUENT_KAFKA and ConfluentConsumer is not None:
            self._consumer = ConfluentConsumer(
                {
                    "bootstrap.servers": self.bootstrap_servers,
                    "group.id": self.group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            self._consumer.subscribe([self.topic])

    def push_in_memory(self, payload: Any, partition: int = 0, offset: Optional[int] = None) -> None:
        next_offset = offset if offset is not None else len(self._buffer)
        self._buffer.append(
            {
                "topic": self.topic,
                "partition": partition,
                "offset": next_offset,
                "value": payload,
            }
        )

    def poll_batch(self, max_records: int = 100, timeout_sec: float = 0.2) -> List[Dict[str, Any]]:
        """Poll up to `max_records` envelopes `{topic, partition, offset, value}`."""
        batch: List[Dict[str, Any]] = []
        while self._buffer and len(batch) < max_records:
            batch.append(self._buffer.popleft())

        if batch or self._use_in_memory or self._consumer is None:
            return batch

        messages = self._consumer.consume(num_messages=max_records, timeout=timeout_sec)
        for msg in messages:
            if msg is None or msg.error():
                continue
            batch.append(
                {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "value": msg.value(),
                }
            )
        return batch

    def commit(self, last_offsets: Optional[Dict[int, int]] = None) -> None:
        if last_offsets:
            self.committed_offsets.update(last_offsets)
        if self._consumer is not None:
            try:
                self._consumer.commit(asynchronous=False)
            except Exception:
                pass

    def close(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None
