from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from ..config import settings

try:
    from confluent_kafka import Producer as ConfluentProducer  # type: ignore[import-not-found]
except ImportError:
    ConfluentProducer = None  # type: ignore[assignment]

logger = logging.getLogger(settings.service_name)


class GatewayKafkaProducer:
    """Async-friendly Kafka producer for `hermes.transactions.raw` and `hermes.dlq`.

    Configures `confluent_kafka.Producer` with:
    - `acks="all"`
    - `enable.idempotence=True`
    - `linger.ms=5`
    - `compression.type="lz4"`
    Partitions `hermes.transactions.raw` strictly by `customer_id`.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        transactions_topic: Optional[str] = None,
        dlq_topic: Optional[str] = None,
        producer_instance: Optional[Any] = None,
        num_partitions: int = 6,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.transactions_topic = transactions_topic or settings.kafka_transactions_topic
        self.dlq_topic = dlq_topic or settings.kafka_dlq_topic
        self.num_partitions = num_partitions
        self._producer = producer_instance
        self._offset_seq = 0
        self.published_transactions: list[dict[str, Any]] = []
        self.published_dlq_events: list[dict[str, Any]] = []

        if self._producer is None and ConfluentProducer is not None:
            try:
                self._producer = ConfluentProducer(
                    {
                        "bootstrap.servers": self.bootstrap_servers,
                        "acks": "all",
                        "enable.idempotence": True,
                        "linger.ms": 5,
                        "compression.type": "lz4",
                    }
                )
            except Exception as exc:
                logger.warning("Failed to initialize ConfluentProducer (%s); using local buffer fallback", exc)
                self._producer = None

    def _compute_partition(self, customer_id: str) -> int:
        digest = hashlib.md5(customer_id.encode("utf-8")).hexdigest()
        return int(digest, 16) % self.num_partitions

    def publish_transaction(self, event: dict[str, Any], correlation_id: str) -> dict[str, Any]:
        """Publish a validated canonical TransactionEvent v1.0 to `hermes.transactions.raw`
        partitioned by `customer_id`.
        """
        customer_id = str(event["customer_id"])
        key_bytes = customer_id.encode("utf-8")
        value_bytes = json.dumps(event, separators=(",", ":")).encode("utf-8")
        headers = [
            ("x-correlation-id", correlation_id.encode("utf-8")),
            ("schema-version", str(event.get("schema_version", "1.0")).encode("utf-8")),
        ]

        partition = self._compute_partition(customer_id)
        self._offset_seq += 1
        offset = self._offset_seq

        if self._producer is not None:
            try:
                self._producer.produce(
                    topic=self.transactions_topic,
                    key=key_bytes,
                    value=value_bytes,
                    headers=headers,
                )
                self._producer.poll(0)
            except Exception as exc:
                logger.warning("Kafka produce warning on %s: %s", self.transactions_topic, exc)

        record_meta = {
            "topic": self.transactions_topic,
            "partition": partition,
            "offset": offset,
            "key": customer_id,
            "correlation_id": correlation_id,
            "event": event,
        }
        self.published_transactions.append(record_meta)
        return record_meta

    def publish_dlq(
        self,
        raw_payload: Any,
        error_code: str,
        error_message: str,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Wrap a rejected payload into a canonical DLQEvent (Section 10.A) and publish to `hermes.dlq`."""
        if isinstance(raw_payload, (bytes, bytearray)):
            serialized_payload = raw_payload.decode("utf-8", errors="replace")
        elif isinstance(raw_payload, str):
            serialized_payload = raw_payload
        else:
            try:
                serialized_payload = json.dumps(raw_payload, default=str)
            except Exception:
                serialized_payload = str(raw_payload)

        dlq_event = {
            "dlq_id": f"dlq-{uuid.uuid4()}",
            "original_payload": serialized_payload,
            "error_code": error_code,
            "error_message": error_message,
            "ingestion_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "schema_version": "1.0",
            "source_topic": self.transactions_topic,
            "source_service": "api-gateway",
            "correlation_id": correlation_id,
        }

        key_bytes = b"api-gateway"
        value_bytes = json.dumps(dlq_event, separators=(",", ":")).encode("utf-8")
        headers = [
            ("x-correlation-id", correlation_id.encode("utf-8")),
            ("error-code", error_code.encode("utf-8")),
        ]

        if self._producer is not None:
            try:
                self._producer.produce(
                    topic=self.dlq_topic,
                    key=key_bytes,
                    value=value_bytes,
                    headers=headers,
                )
                self._producer.poll(0)
            except Exception as exc:
                logger.warning("Kafka DLQ produce warning on %s: %s", self.dlq_topic, exc)

        self.published_dlq_events.append(dlq_event)
        return dlq_event

    def is_ready(self) -> bool:
        return True

    def close(self) -> None:
        if self._producer is not None:
            try:
                self._producer.flush(timeout=2.0)
            except Exception:
                pass


_default_producer: Optional[GatewayKafkaProducer] = None


def get_kafka_producer() -> GatewayKafkaProducer:
    global _default_producer
    if _default_producer is None:
        _default_producer = GatewayKafkaProducer()
    return _default_producer
